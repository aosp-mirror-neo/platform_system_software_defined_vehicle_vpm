//
// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#![allow(dead_code)]

use crate::setup::binder::BinderFeatures;
use crate::setup::binder::Strong;
use com_sdv_google_vpm_client_power_notification_service_rpc::Interface as NotificationInterface;
use com_sdv_google_vpm_vpm_system_service_rpc::Client;
use futures_util::StreamExt;
use google_sdv_vpm::aidl::google::sdv::vpm::IPowerStateClientApi::{
    BpPowerStateClientApi, IPowerStateClientApi,
};
use google_sdv_vpm::aidl::google::sdv::vpm::IPowerStateReportListener::{
    BnPowerStateReportListener, IPowerStateReportListener,
};
use google_sdv_vpm::aidl::google::sdv::vpm::PowerStateReport::PowerStateReport as PowerStateReportAidl;
use google_sdv_vpm::binder;
use rustutils::android::system_properties::read;
use sdv_comms::id::ServiceFqin;
use sdv_comms::ContextRef;
use sdv_mw_rs_comms::ClientOptions;
use sdv_mw_rs_comms::RpcClient;
use sdv_mw_rs_comms::SubscribeOptions;
use sdv_mw_rs_comms::UnitName;
use sdv_mw_rs_sdv_comms::SdvComms;
use sdv_mw_rs_test_sdv_vpm_vpm_system_client::subscriber;
use sdv_mw_rs_test_sdv_vpm_vpm_system_client::VpmSystemClient;
use sdv_vpm_api::power::PowerStateReport;
use sdv_vpm_api::vehicle::VpmVehicleState;
use sdv_vpm_api::vpm::GetPowerStateRequest;
use sdv_vpm_api::vpm::PowerStateChangeRequest;
use sdv_vpm_api::vpm::PowerStateRequest;
use sdv_vpm_api::vpm::SetVehicleStateRequest;
use sdv_vpm_api::vpm::ShutdownType;
use sdv_vpm_api::vpm_notifications::power_state_notification_request::PowerStateChangeReason;
use sdv_vpm_api::vpm_notifications::PowerStateNotificationRequest;
use sdv_vpm_api::vpm_notifications::PowerStateNotificationResponse;
use sdv_vpm_power_persistency_tags::*;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::sync::mpsc::{
    channel as tokio_channel, Receiver as TokioReceiver, Sender as TokioSender,
};
use tokio::time::timeout;

pub const TIMEOUT_IN_SECS: Duration = Duration::from_secs(30);
pub const OEM_NOTIFICATION_CHANNEL_SIZE: usize = 1;

/* Valid combinations of reported power state, and vpm internal power states persisted on disk */
const VALID_REPORTED_TO_PERSISTED_TABLE: [(PowerStateReport, &str); 17] = [
    (PowerStateReport::POWER_OFF_EXIT, POWER_OFF_EXIT_TAG),
    (PowerStateReport::POWER_OFF_EXIT, WAIT_FOR_FINISH_POWER_OFF_TAG),
    (PowerStateReport::SUSPEND_TO_RAM_EXIT, SUSPEND_TO_RAM_EXIT_TAG),
    (PowerStateReport::SUSPEND_TO_DISK_EXIT, SUSPEND_TO_DISK_EXIT_TAG),
    (PowerStateReport::ON, ON_TAG),
    (PowerStateReport::POWER_OFF_ENTER, POWER_OFF_ENTER_TAG),
    (PowerStateReport::SUSPEND_TO_RAM_ENTER, SUSPEND_TO_RAM_ENTER_TAG),
    (PowerStateReport::SUSPEND_TO_RAM_ENTER, WAIT_FOR_FINISH_SUSPEND_TO_RAM_TAG),
    (PowerStateReport::SUSPEND_TO_DISK_ENTER, SUSPEND_TO_DISK_ENTER_TAG),
    (PowerStateReport::SUSPEND_TO_DISK_ENTER, WAIT_FOR_FINISH_SUSPEND_TO_DISK_TAG),
    (PowerStateReport::WAIT_FOR_FINISH, WAIT_FOR_FINISH_POWER_OFF_TAG),
    (PowerStateReport::WAIT_FOR_FINISH, WAIT_FOR_FINISH_SUSPEND_TO_DISK_TAG),
    (PowerStateReport::WAIT_FOR_FINISH, WAIT_FOR_FINISH_SUSPEND_TO_RAM_TAG),
    (PowerStateReport::SHUTDOWN_CANCELLED, SHUTDOWN_CANCELLED_TAG),
    (PowerStateReport::POWER_OFF_POST_FINISH, POWER_OFF_POST_FINISH_TAG),
    (PowerStateReport::SUSPEND_TO_RAM_POST_FINISH, SUSPEND_TO_RAM_POST_FINISH_TAG),
    (PowerStateReport::SUSPEND_TO_DISK_POST_FINISH, SUSPEND_TO_DISK_POST_FINISH_TAG),
];

pub struct TestContext {
    pub vepsm: TestVePSM,
    pub power_callback_preparer: PowerCallbackPreparer,
}

impl TestContext {
    pub async fn new(instance_name: String) -> Self {
        Self {
            vepsm: TestVePSM::new(instance_name).await,
            power_callback_preparer: PowerCallbackPreparer::new(),
        }
    }
    pub fn is_good_start_state(&self) -> bool {
        self.vepsm.power_state == PowerStateReport::POWER_OFF_EXIT
            || self.vepsm.power_state == PowerStateReport::SHUTDOWN_CANCELLED
    }

    // Put VM state in SHUTDOWN_CANCELLED because it's a BOOTUP_PREPARE state.
    // We can't put the state in POWER_OFF_EXIT because it can only be set in cold boot.
    pub async fn async_drop(&mut self) {
        if self.vepsm.power_state == PowerStateReport::ON {
            let request = PowerStateChangeRequest {
                power_request: PowerStateRequest::PREPARE_SHUTDOWN.into(),
                shutdown_type: ShutdownType::SUSPEND_TO_RAM.into(),
                timeout_ms: 10000,
                ..Default::default()
            };

            self.vepsm.send_power_state_request(request).await;

            self.vepsm
                .assert_power_notification(
                    PowerStateReport::SUSPEND_TO_RAM_ENTER,
                    PowerStateChangeReason::HOST_REQUESTED,
                )
                .await;

            self.vepsm
                .assert_power_notification(
                    PowerStateReport::WAIT_FOR_FINISH,
                    PowerStateChangeReason::HOST_REQUESTED,
                )
                .await;

            let request = PowerStateChangeRequest {
                power_request: PowerStateRequest::CANCEL_SHUTDOWN.into(),
                timeout_ms: 10000,
                ..Default::default()
            };
            self.vepsm.send_power_state_request(request).await;

            self.vepsm
                .assert_power_notification(
                    PowerStateReport::SHUTDOWN_CANCELLED,
                    PowerStateChangeReason::HOST_REQUESTED,
                )
                .await;
        }
    }
}

pub struct PowerCallbackPreparer {
    // The callback receiver will wait 20 seconds.
    // if power state report is set.
    sleep_callback_if_power_state: Option<PowerStateReportAidl>,
    fail_callback: bool,
    callback_sender: Sender<PowerStateReportAidl>,
    callback_receiver: Receiver<PowerStateReportAidl>,
}

impl PowerCallbackPreparer {
    pub fn new() -> Self {
        let (callback_sender, callback_receiver): (
            Sender<PowerStateReportAidl>,
            Receiver<PowerStateReportAidl>,
        ) = channel();
        Self {
            sleep_callback_if_power_state: None,
            fail_callback: false,
            callback_sender,
            callback_receiver,
        }
    }
    pub fn fail_callback(&mut self) -> &mut PowerCallbackPreparer {
        self.fail_callback = true;
        self
    }

    pub fn with_sleep_on_state(
        &mut self,
        power_state_report: PowerStateReportAidl,
    ) -> &mut PowerCallbackPreparer {
        self.sleep_callback_if_power_state = Some(power_state_report);
        self
    }

    pub fn assert_callback_received(&self, power_state_report: PowerStateReportAidl) {
        let callback_result =
            self.callback_receiver.recv_timeout(TIMEOUT_IN_SECS).expect("Timeout reached");
        assert_eq!(callback_result, power_state_report);
    }

    pub fn setup_binder_callbacks(&self) {
        binder::ProcessState::start_thread_pool();
        let cloned_sender = self.callback_sender.clone();
        let test_sdv_agent = TestSdvAgent {
            callback_sender: cloned_sender,
            sleep_callback_if_power_state: self.sleep_callback_if_power_state,
            fail_callback: self.fail_callback,
        };
        let power_state_client_interface =
            get_power_state_client_interface().expect("Cannot find VPM client binder service");
        let power_report_listener =
            BnPowerStateReportListener::new_binder(test_sdv_agent, BinderFeatures::default());
        power_state_client_interface.subscribeToPowerStateReport(&power_report_listener).unwrap();
    }
}

fn get_power_state_client_interface() -> anyhow::Result<Strong<dyn IPowerStateClientApi>> {
    let descriptor =
        <BpPowerStateClientApi as IPowerStateClientApi>::get_descriptor().to_owned() + "/default";
    Ok(binder::wait_for_interface::<dyn IPowerStateClientApi>(&descriptor)?)
}

pub struct TestSdvAgent {
    callback_sender: Sender<PowerStateReportAidl>,
    sleep_callback_if_power_state: Option<PowerStateReportAidl>,
    fail_callback: bool,
}

impl binder::Interface for TestSdvAgent {}

impl IPowerStateReportListener for TestSdvAgent {
    fn onPowerStateReportChanged(&self, power_state: PowerStateReportAidl) -> binder::Result<()> {
        if self.sleep_callback_if_power_state.is_some()
            && self.sleep_callback_if_power_state.unwrap() == power_state
        {
            // The time is to simulate the caller took to long to handle the power state change.
            thread::sleep(Duration::from_secs(20));
            if self.fail_callback {
                return Err(binder::StatusCode::UNKNOWN_ERROR.into());
            }
        }
        self.callback_sender.send(power_state).unwrap();
        Ok(())
    }
}

pub struct TestVePSM {
    power_request_receiver: TokioReceiver<PowerStateNotificationRequest>,
    vehicle_callback_receiver: Receiver<VpmVehicleState>,
    client: Arc<Client>,
    bundle: VpmSystemClient,
    power_state: PowerStateReport,
}

impl TestVePSM {
    pub async fn new(instance_name: String) -> TestVePSM {
        let (vehicle_request_sender, vehicle_callback_receiver): (
            Sender<VpmVehicleState>,
            Receiver<VpmVehicleState>,
        ) = channel();
        let (power_request_sender, power_request_receiver): (
            TokioSender<PowerStateNotificationRequest>,
            TokioReceiver<PowerStateNotificationRequest>,
        ) = tokio_channel(OEM_NOTIFICATION_CHANNEL_SIZE);

        let power_notification_receiver =
            PowerNotificationReceiver { sender: power_request_sender };

        let context = create_context(instance_name);
        let comms = SdvComms { context };
        let mut bundle =
            VpmSystemClient::new(Arc::new(comms.clone()), (Arc::new(power_notification_receiver),))
                .await
                .unwrap();
        let client = bundle
            .create_rpc_client(
                UnitName::builder()
                    .package_name("com.sdv.google.vpm")
                    .bundle_name("VpmSystemServiceBundle")
                    .vm_name(comms.context.get_self_fqin().get_sdv_vm_name())
                    .service_unit_name(
                        com_sdv_google_vpm_vpm_system_service_rpc::Client::DEFAULT_UNIT_NAME,
                    )
                    .build()
                    .unwrap(),
                ClientOptions::default(),
            )
            .await
            .unwrap();

        let subscriber_options = SubscribeOptions::builder()
            .vm_name(comms.context.get_self_fqin().get_sdv_vm_name().to_string())
            .build()
            .unwrap();
        let mut subscriber = bundle
            .create_observer(
                subscriber::Variant::<sdv_vpm_api::vehicle::VehicleStateChange>::UNIQUE,
                &subscriber_options,
            )
            .await
            .unwrap();

        tokio::spawn(async move {
            while let Some(Ok(vehicle_state_changes)) = subscriber.next().await {
                for vehicle_state_change in vehicle_state_changes {
                    vehicle_request_sender
                        .send(vehicle_state_change.vehicle_state.unwrap())
                        .unwrap();
                }
            }
        });

        Self {
            power_request_receiver,
            vehicle_callback_receiver,
            client: Arc::new(client),
            bundle,
            power_state: PowerStateReport::POWER_OFF_EXIT,
        }
    }

    pub async fn send_power_state_request(&self, request: PowerStateChangeRequest) {
        self.client.SetPowerState(&request).await.unwrap();
    }

    pub async fn get_current_power_state(&self) -> PowerStateReport {
        let response =
            self.client.GetPowerState(&GetPowerStateRequest { ..Default::default() }).await;
        response.unwrap().power_state.unwrap()
    }

    pub async fn send_vehicle_state_request(&self, request: SetVehicleStateRequest) {
        self.client.SetVehicleState(&request).await.unwrap();
    }

    pub fn assert_vehicle_state_received(&self, vehicle_state: VpmVehicleState) {
        let result =
            self.vehicle_callback_receiver.recv_timeout(TIMEOUT_IN_SECS).expect("Timeout reached");
        assert_eq!(result, vehicle_state);
    }

    pub async fn assert_power_notification(
        &mut self,
        power_state_report: PowerStateReport,
        reason: PowerStateChangeReason,
    ) {
        if let Ok(Some(power_notification)) =
            timeout(TIMEOUT_IN_SECS, self.power_request_receiver.recv()).await
        {
            assert_eq!(power_notification.power_state.unwrap(), power_state_report);
            assert_eq!(power_notification.reason.unwrap(), reason);
            Self::assert_matches_disk_persisted_state(power_state_report);
            self.power_state = power_state_report;
        } else {
            panic!("Timeout reached waiting for OEM notification");
        }
    }

    fn assert_matches_disk_persisted_state(power_state_report: PowerStateReport) {
        let persisted_power_state = read(PERSISTED_POWER_STATE_PROP)
            .ok()
            .flatten()
            .expect("Could not read property. Is test running as root?");
        assert!(
            VALID_REPORTED_TO_PERSISTED_TABLE
                .contains(&(power_state_report, persisted_power_state.as_str())),
            "Unexpected reported-persisted state pair encountered: {:?}",
            (power_state_report, persisted_power_state)
        );
    }
}

struct PowerNotificationReceiver {
    sender: TokioSender<PowerStateNotificationRequest>,
}

#[async_trait::async_trait]
impl NotificationInterface for PowerNotificationReceiver {
    async fn NotifyPowerState(
        &self,
        _caller_id: sdv_comms::id::ServiceFqin,
        request: &sdv_vpm_api::vpm_notifications::PowerStateNotificationRequest,
    ) -> sdv_status_rs::SdvResult<sdv_vpm_api::vpm_notifications::PowerStateNotificationResponse>
    {
        self.sender.send(request.clone()).await.unwrap();
        Ok(PowerStateNotificationResponse { ..Default::default() })
    }
}

fn create_context(instance_name: String) -> ContextRef {
    let fqin = ServiceFqin::builder()
        .sdv_vm_name("vm")
        .sdv_package_name("com.sdv.google.vpm.client")
        .service_bundle_name("VpmSystemClient")
        .service_instance_name(instance_name)
        .build()
        .unwrap();

    ContextRef::create(fqin)
}
