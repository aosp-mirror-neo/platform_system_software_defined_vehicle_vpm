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

use crate::power_state_notifier::binder::BinderFeatures;
use crate::power_state_notifier::binder::ExceptionCode;
use crate::power_state_notifier::binder::Status;
use crate::power_state_notifier::binder::ThreadState;
use crate::PowerState;
use anyhow::anyhow;
use async_trait::async_trait;
use binder::{DeathRecipient, IBinder};
use binder_tokio::Tokio;
use binder_tokio::TokioRuntime;
use com_android_sdv_vpm_client_power_notification_service_rpc::Client;
use google_sdv_vpm::aidl::google::sdv::vpm::IPowerStateClientApi::IPowerStateClientApiAsyncServer;
use google_sdv_vpm::aidl::google::sdv::vpm::IPowerStateClientApi::{
    BnPowerStateClientApi, BpPowerStateClientApi, IPowerStateClientApi,
};
use google_sdv_vpm::aidl::google::sdv::vpm::IPowerStateReportListener::IPowerStateReportListener;
use google_sdv_vpm::aidl::google::sdv::vpm::IPowerStateReportListener::IPowerStateReportListenerAsync;
use google_sdv_vpm::aidl::google::sdv::vpm::PowerStateReport::PowerStateReport as PowerStateReportAidl;
use google_sdv_vpm::binder;
use google_sdv_vpm::binder::{Interface, Result as BinderResult, Strong};
use log::{debug, error, info};
use mockall::automock;
use sdv_mw_rs_comms::{ClientAttributes, ClientOptions, Communicate, RpcClient, UnitName};
use sdv_status_rs::SdvStatusCode;
use sdv_vpm_api::power::PowerStateReport;
use sdv_vpm_api::vpm_notifications::power_state_notification_request::PowerStateChangeReason;
use sdv_vpm_api::vpm_notifications::PowerStateNotificationRequest;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::sync::watch::Receiver as WatchReceiver;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinSet;
use tracing::instrument;

const OEM_NOTIFICATION_TIMEOUT_SECS: u32 = 30;

#[async_trait]
#[automock]
pub trait PowerStateNotifier: Send + Sync {
    fn create_binder_service(
        &self,
        power_state_watch_receiver: WatchReceiver<Arc<dyn PowerState>>,
    ) -> anyhow::Result<()>;
    async fn notify(
        &self,
        power_state_report: PowerStateReport,
    ) -> anyhow::Result<(), PowerStateNotificationError>;
    async fn notify_oem(
        &self,
        power_state_report: PowerStateReport,
        reason: PowerStateChangeReason,
    ) -> anyhow::Result<()>;
}

#[derive(Error, Debug, Clone)]
pub enum PowerStateNotificationError {
    #[error("Notification was cancelled by caller!")]
    Cancelled,
    #[error("Callback client was disconnected!")]
    Disconnected,
    #[error("Timeout was reached while notifying callbacks!")]
    TimedOut,
    #[error("Failed to deliver callback!")]
    Failed,
}

#[derive(Clone)]
pub struct PowerStateNotifierImpl {
    // Maps the AID of the agent with the callback.
    callbacks: Arc<Mutex<HashMap<u32, ClientCallback>>>,
    comms: Arc<dyn Communicate>,
    vm_name: String,
    notification_rpc_client: Arc<AsyncMutex<Option<Arc<Client>>>>,
}

impl PowerStateNotifierImpl {
    pub fn new(comms: Arc<dyn Communicate>, vm_name: &str) -> Self {
        Self {
            callbacks: Arc::new(Mutex::new(HashMap::new())),
            comms,
            vm_name: vm_name.to_string(),
            notification_rpc_client: Arc::new(AsyncMutex::new(None)),
        }
    }

    pub async fn get_notification_client(&self) -> anyhow::Result<Arc<Client>> {
        let client_guard = self.notification_rpc_client.lock().await;
        let client = client_guard.clone();
        client.ok_or_else(|| anyhow!("No notification client is stored"))
    }

    async fn update_client(&self) -> anyhow::Result<()> {
        let mut client = self.notification_rpc_client.lock().await;
        let attributes = self.get_attributes()?;
        let result = self.comms.create_client_transport(&attributes).await;
        let Ok(result) = result else {
            error!("Could not get a client for vpm notifications");
            return Err(anyhow!("Could not get a client for vpm notifications"));
        };
        *client = Some(Client::new(result).into());
        Ok(())
    }

    fn get_attributes(&self) -> anyhow::Result<ClientAttributes> {
        let server_name = get_server_unit_name(
            self.vm_name.as_str(),
            "com.android.sdv.vpm.client",
            "VpmSystemClient",
            Client::DEFAULT_UNIT_NAME,
        )?;
        let client_options =
            ClientOptions::builder().timeout_in_sec(OEM_NOTIFICATION_TIMEOUT_SECS).build()?;
        ClientAttributes::builder()
            .server_name(server_name)
            .client_options(client_options)
            .build()
            .map_err(|e| anyhow!("invalid ClientAttributes: {e:?}"))
    }
}

#[async_trait]
impl PowerStateNotifier for PowerStateNotifierImpl {
    fn create_binder_service(
        &self,
        power_state_watch_receiver: WatchReceiver<Arc<dyn PowerState>>,
    ) -> anyhow::Result<()> {
        info!("VPM client binder starting!");
        let client_api_binder = BnPowerStateClientApi::new_async_binder(
            BinderServer::new(
                self.callbacks.clone(),
                power_state_watch_receiver,
                Arc::new(DeathLinkerImpl {}),
            ),
            TokioRuntime(Handle::current()),
            BinderFeatures::default(),
        );
        let descriptor = <BpPowerStateClientApi as IPowerStateClientApi>::get_descriptor()
            .to_owned()
            + "/default";
        binder::add_service(&descriptor, client_api_binder.as_binder())?;
        Ok(())
    }

    #[instrument(skip(self))]
    async fn notify(
        &self,
        power_state_report: PowerStateReport,
    ) -> anyhow::Result<(), PowerStateNotificationError> {
        info!("Starting callback notifications for {:?}", power_state_report);

        let callbacks: Vec<_> = self
            .callbacks
            .lock()
            .unwrap()
            .iter()
            .map(|(id, client_callback)| (*id, client_callback.async_callback.clone()))
            .collect();

        let mut set = JoinSet::new();
        for (id, async_callback) in callbacks.into_iter() {
            set.spawn(async move {
                debug!("Starting callback notification for {id:?}");
                let result = async_callback
                    .onPowerStateReportChanged(convert_to_aidl(power_state_report).map_err(
                        |e| {
                            error!("Failed to convert power state report to AIDL: {e:?}");
                            PowerStateNotificationError::Failed
                        },
                    )?)
                    .await;
                if result.is_err() {
                    Err(PowerStateNotificationError::Disconnected)
                } else {
                    Ok(())
                }
            });
        }

        if notification_failed(set).await {
            return Err(PowerStateNotificationError::Failed);
        } else {
            Ok(())
        }
    }

    #[instrument(skip(self))]
    async fn notify_oem(
        &self,
        power_state_report: PowerStateReport,
        reason: PowerStateChangeReason,
    ) -> anyhow::Result<()> {
        let request = PowerStateNotificationRequest {
            power_state: power_state_report.into(),
            reason: reason.into(),
            ..Default::default()
        };

        if self.get_notification_client().await.is_err() {
            debug!("Updating the notification client");
            self.update_client().await?;
            debug!("Successfully created a new OEM notification client");
        }

        // Call the notification server.
        let client = self.get_notification_client().await?;
        let mut result = client.NotifyPowerState(&request).await;

        // Only retry if the client was unavailable.
        if let Err(ref sdv_status) = result {
            if sdv_status.status_code() == SdvStatusCode::Unavailable {
                debug!("Updating the notification client because it was unavailable");
                self.update_client().await?;
                result = self.get_notification_client().await?.NotifyPowerState(&request).await;
            }
        }
        if result.is_err() {
            return Err(anyhow!("Request to notify VPM client failed {:?}", result));
        }
        Ok(())
    }
}

#[automock]
trait DeathLinker: Send + Sync {
    fn link_callback_to_death_recipient(
        &self,
        callback: &Strong<dyn IPowerStateReportListener>,
        death_recipient: &mut DeathRecipient,
    ) -> BinderResult<()> {
        Ok(callback.as_binder().clone().link_to_death(death_recipient)?)
    }
}

struct DeathLinkerImpl {}
impl DeathLinker for DeathLinkerImpl {}

struct BinderServer {
    callbacks: Arc<Mutex<HashMap<u32, ClientCallback>>>,
    power_state_watch_receiver: WatchReceiver<Arc<dyn PowerState>>,
    death_linker: Arc<dyn DeathLinker>,
}

impl BinderServer {
    fn new(
        callbacks: Arc<Mutex<HashMap<u32, ClientCallback>>>,
        power_state_watch_receiver: WatchReceiver<Arc<dyn PowerState>>,
        death_linker: Arc<dyn DeathLinker>,
    ) -> Self {
        Self { callbacks, power_state_watch_receiver, death_linker }
    }
}
impl Interface for BinderServer {}

#[allow(dead_code)]
struct ClientCallback {
    async_callback: Strong<dyn IPowerStateReportListenerAsync<Tokio>>,
    // death_recipient has to be kept alive as long as the callback event if not used.
    death_recipient: DeathRecipient,
}

#[async_trait]
impl IPowerStateClientApiAsyncServer for BinderServer {
    async fn subscribeToPowerStateReport(
        &self,
        callback: &Strong<dyn IPowerStateReportListener>,
    ) -> BinderResult<PowerStateReportAidl> {
        info!("VPM Agent received client request to subscribe to power updates!");
        let id = ThreadState::get_calling_uid();
        let callbacks_clone = self.callbacks.clone();
        let mut death_recipient = DeathRecipient::new(move || {
            on_client_died(callbacks_clone.clone(), id);
        });
        let result =
            self.death_linker.link_callback_to_death_recipient(callback, &mut death_recipient);
        if result.is_err() {
            error!("Failed to link death binder {result:?}");
            return Err(Status::new_exception(ExceptionCode::SERVICE_SPECIFIC, None));
        }

        debug!("Death binder linked successfully");
        let client_callback =
            ClientCallback { async_callback: callback.clone().into_async(), death_recipient };

        self.callbacks.lock().unwrap().insert(id, client_callback);
        let power_state = self.power_state_watch_receiver.borrow();
        convert_to_aidl(power_state.get_report()).map_err(|e| {
            error!("Failed to convert power state report to AIDL: {e:?}");
            Status::new_exception(ExceptionCode::SERVICE_SPECIFIC, None)
        })
    }
}

fn on_client_died(callbacks: Arc<Mutex<HashMap<u32, ClientCallback>>>, id: u32) {
    info!("Handling death notification for id {id:?}");
    callbacks.lock().unwrap().remove(&id);
}

fn convert_to_aidl(power_state_report: PowerStateReport) -> anyhow::Result<PowerStateReportAidl> {
    Ok(match power_state_report {
        PowerStateReport::POWER_OFF_EXIT => PowerStateReportAidl::POWER_OFF_EXIT,
        PowerStateReport::SUSPEND_TO_RAM_EXIT => PowerStateReportAidl::SUSPEND_TO_RAM_EXIT,
        PowerStateReport::SUSPEND_TO_DISK_EXIT => PowerStateReportAidl::SUSPEND_TO_DISK_EXIT,
        PowerStateReport::ON => PowerStateReportAidl::ON,
        PowerStateReport::SHUTDOWN_CANCELLED => PowerStateReportAidl::SHUTDOWN_CANCELLED,
        PowerStateReport::POWER_OFF_ENTER => PowerStateReportAidl::POWER_OFF_ENTER,
        PowerStateReport::SUSPEND_TO_RAM_ENTER => PowerStateReportAidl::SUSPEND_TO_RAM_ENTER,
        PowerStateReport::SUSPEND_TO_DISK_ENTER => PowerStateReportAidl::SUSPEND_TO_DISK_ENTER,
        PowerStateReport::POWER_OFF_POST_FINISH => PowerStateReportAidl::POWER_OFF_POST_FINISH,
        PowerStateReport::SUSPEND_TO_RAM_POST_FINISH => {
            PowerStateReportAidl::SUSPEND_TO_RAM_POST_FINISH
        }
        PowerStateReport::SUSPEND_TO_DISK_POST_FINISH => {
            PowerStateReportAidl::SUSPEND_TO_DISK_POST_FINISH
        }
        PowerStateReport::WAIT_FOR_FINISH => PowerStateReportAidl::WAIT_FOR_FINISH,
        PowerStateReport::REPORT_UNSPECIFIED => {
            error!("This conversion should never be called!");
            return Err(anyhow!("This conversion, convert_to_aidl(PowerStateReport::REPORT_UNSPECIFIED), should never be called!"));
        }
    })
}

fn get_server_unit_name(
    vm_name: &str,
    package_name: &str,
    bundle_name: &str,
    service_unit_name: &str,
) -> anyhow::Result<UnitName> {
    UnitName::builder()
        .vm_name(vm_name.to_string())
        .package_name(package_name.to_string())
        .bundle_name(bundle_name.to_string())
        .service_unit_name(service_unit_name)
        .build()
        .map_err(|e| anyhow!("invalid server UnitName for vm_name {vm_name}, package_name {package_name}, bundle_name {bundle_name}, service_unit_name {service_unit_name}: {e:?}"))
}

async fn notification_failed(mut set: JoinSet<Result<(), PowerStateNotificationError>>) -> bool {
    let mut notification_failed = false;
    while let Some(handle) = set.join_next().await {
        if handle.is_err() {
            error!("Power notification callback task failed, {:?}", handle.err());
            notification_failed = true;
            continue;
        }
        if let Ok(Err(result)) = handle {
            error!("Power notification callback request failed, {result:?}");
            notification_failed = true;
        }
    }
    notification_failed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::power_state::PowerOffExit;
    use crate::power_state_notifier::binder::BinderFeatures;
    use futures::Stream;
    use google_sdv_vpm::aidl::google::sdv::vpm::IPowerStateReportListener::{
        BnPowerStateReportListener, MockIPowerStateReportListener,
    };
    use google_sdv_vpm::aidl::google::sdv::vpm::PowerStateReport::PowerStateReport as PowerStateReportAidl;
    use sdv_mw_rs_comms::{ClientTransport, MockCommunicate};
    use sdv_status_rs::SdvResult;
    use sdv_status_rs::SdvStatus;
    use sdv_vpm_api::power::PowerStateReport;
    use std::pin::Pin;

    mockall::mock! {
        TestClientTransport {}
        #[async_trait::async_trait]
        impl ClientTransport for TestClientTransport {
            async fn call_method(&self, method: &str, request: &[u8]) -> SdvResult<Vec<u8>>;
            async fn call_client_streaming_method(
                &self,
                method_name: &str,
                request_stream: Pin<Box<dyn Stream<Item = Vec<u8>> + Send>>,
            ) -> SdvResult<Vec<u8>>;
            async fn call_server_streaming_method(
                &self,
                method_name: &str,
                request: &[u8],
            ) -> SdvResult<Pin<Box<dyn Stream<Item = SdvResult<Vec<u8>>> + Send>>>;
        }
    }

    #[test]
    fn test_convert_to_aidl_valid_conversions() {
        assert_eq!(
            convert_to_aidl(PowerStateReport::POWER_OFF_EXIT).unwrap(),
            PowerStateReportAidl::POWER_OFF_EXIT
        );
        assert_eq!(
            convert_to_aidl(PowerStateReport::SUSPEND_TO_RAM_EXIT).unwrap(),
            PowerStateReportAidl::SUSPEND_TO_RAM_EXIT
        );
        assert_eq!(
            convert_to_aidl(PowerStateReport::SUSPEND_TO_DISK_EXIT).unwrap(),
            PowerStateReportAidl::SUSPEND_TO_DISK_EXIT
        );
        assert_eq!(convert_to_aidl(PowerStateReport::ON).unwrap(), PowerStateReportAidl::ON);
        assert_eq!(
            convert_to_aidl(PowerStateReport::SHUTDOWN_CANCELLED).unwrap(),
            PowerStateReportAidl::SHUTDOWN_CANCELLED
        );
        assert_eq!(
            convert_to_aidl(PowerStateReport::POWER_OFF_ENTER).unwrap(),
            PowerStateReportAidl::POWER_OFF_ENTER
        );
        assert_eq!(
            convert_to_aidl(PowerStateReport::SUSPEND_TO_RAM_ENTER).unwrap(),
            PowerStateReportAidl::SUSPEND_TO_RAM_ENTER
        );
        assert_eq!(
            convert_to_aidl(PowerStateReport::SUSPEND_TO_DISK_ENTER).unwrap(),
            PowerStateReportAidl::SUSPEND_TO_DISK_ENTER
        );
        assert_eq!(
            convert_to_aidl(PowerStateReport::POWER_OFF_POST_FINISH).unwrap(),
            PowerStateReportAidl::POWER_OFF_POST_FINISH
        );
        assert_eq!(
            convert_to_aidl(PowerStateReport::SUSPEND_TO_RAM_POST_FINISH).unwrap(),
            PowerStateReportAidl::SUSPEND_TO_RAM_POST_FINISH
        );
        assert_eq!(
            convert_to_aidl(PowerStateReport::SUSPEND_TO_DISK_POST_FINISH).unwrap(),
            PowerStateReportAidl::SUSPEND_TO_DISK_POST_FINISH
        );
        assert_eq!(
            convert_to_aidl(PowerStateReport::WAIT_FOR_FINISH).unwrap(),
            PowerStateReportAidl::WAIT_FOR_FINISH
        );
    }

    #[test]
    fn test_convert_to_aidl_report_unspecified() {
        let input = PowerStateReport::REPORT_UNSPECIFIED;
        assert!(convert_to_aidl(input).is_err());
    }

    #[test]
    fn test_get_attributes() {
        // Given a PowerStateNotifierImpl with a specific vm_name
        let vm_name = "test_vm";
        let notifier = PowerStateNotifierImpl::new(Arc::new(MockCommunicate::new()), vm_name);

        // When calling get_attributes
        let attributes_result = notifier.get_attributes();

        // Then the result should be Ok
        assert!(attributes_result.is_ok());
        let attributes = attributes_result.unwrap();

        // And the server_name should be correctly constructed
        let server_name = attributes.get_server_name();
        assert_eq!(server_name.get_vm_name(), vm_name);
        assert_eq!(server_name.get_package_name(), "com.android.sdv.vpm.client");
        assert_eq!(server_name.get_bundle_name(), "VpmSystemClient");
        assert_eq!(server_name.get_service_unit_name(), Client::DEFAULT_UNIT_NAME);

        // And the client_options should have the correct timeout
        let client_options = attributes.get_client_options();
        assert_eq!(client_options.get_timeout_in_sec(), OEM_NOTIFICATION_TIMEOUT_SECS);
    }

    #[tokio::test]
    async fn power_state_notifier_impl_get_notification_client_when_none() {
        let notifier = PowerStateNotifierImpl::new(Arc::new(MockCommunicate::new()), "test");

        let result = notifier.get_notification_client().await;
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().to_string(), "No notification client is stored");
    }

    #[tokio::test]
    async fn power_state_notifier_impl_get_notification_updates_client() {
        let mut mock_comms = MockCommunicate::new();
        mock_comms.expect_create_client_transport().times(1).returning(|_| {
            let mock_client_transport = MockTestClientTransport::new();
            Box::pin(async move { Ok(Box::new(mock_client_transport) as Box<dyn ClientTransport>) })
        });
        let notifier = PowerStateNotifierImpl::new(Arc::new(mock_comms), "test");

        let result = notifier.update_client().await;
        let client = notifier.get_notification_client().await;
        assert!(result.is_ok());
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn power_state_notifier_impl_update_client_fails() {
        let mut mock_comms = MockCommunicate::new();
        mock_comms
            .expect_create_client_transport()
            .times(1)
            .returning(|_| Box::pin(async move { Err(SdvStatus::new(SdvStatusCode::Unknown)) }));
        let notifier = PowerStateNotifierImpl::new(Arc::new(mock_comms), "test");

        let update_result = notifier.update_client().await;
        assert!(update_result.is_err());
        assert_eq!(
            update_result.err().unwrap().to_string(),
            "Could not get a client for vpm notifications"
        );

        let client_result = notifier.get_notification_client().await;
        assert!(client_result.is_err());
        assert_eq!(client_result.err().unwrap().to_string(), "No notification client is stored");
    }

    #[tokio::test]
    async fn test_subscribe_to_power_state_report_success() {
        let callbacks_map = Arc::new(Mutex::new(HashMap::new()));
        let power_off_exit_state = Arc::new(PowerOffExit {});
        let (_tx, rx) =
            tokio::sync::watch::channel(power_off_exit_state.clone() as Arc<dyn PowerState>);
        let mut mock_death_linker = MockDeathLinker::default();
        mock_death_linker
            .expect_link_callback_to_death_recipient()
            .times(1)
            .returning(|_, _| Ok(()));
        let server = BinderServer::new(callbacks_map.clone(), rx, Arc::new(mock_death_linker));
        let fake_listener_impl = MockIPowerStateReportListener::new();
        let fake_listener_binder =
            BnPowerStateReportListener::new_binder(fake_listener_impl, BinderFeatures::default());

        let result = server.subscribeToPowerStateReport(&fake_listener_binder).await;

        assert!(result.is_ok(), "subscribeToPowerStateReport failed: {:?}", result.err());
        let returned_report_aidl = result.unwrap();
        assert_eq!(
            returned_report_aidl,
            convert_to_aidl(PowerStateReport::POWER_OFF_EXIT).unwrap()
        );
        assert_eq!(callbacks_map.lock().unwrap().len(), 1, "Callback not added to map");
    }

    #[tokio::test]
    async fn test_subscribe_to_power_state_report_link_death_fails() {
        let callbacks_map = Arc::new(Mutex::new(HashMap::new()));
        let power_off_exit_state = Arc::new(PowerOffExit {});
        let (_tx, rx) =
            tokio::sync::watch::channel(power_off_exit_state.clone() as Arc<dyn PowerState>);
        let server = BinderServer::new(callbacks_map.clone(), rx, Arc::new(DeathLinkerImpl {}));
        let fake_listener_impl = MockIPowerStateReportListener::new();
        let fake_listener_binder =
            BnPowerStateReportListener::new_binder(fake_listener_impl, BinderFeatures::default());

        let result = server.subscribeToPowerStateReport(&fake_listener_binder).await;

        assert!(result.is_err(), "subscribeToPowerStateReport should have failed");
        let error_status = result.err().unwrap();
        assert_eq!(error_status.exception_code(), ExceptionCode::SERVICE_SPECIFIC);
        assert_eq!(
            callbacks_map.lock().unwrap().len(),
            0,
            "Callback should not be added if linking death recipient fails"
        );
    }

    #[tokio::test]
    async fn test_notify_no_listeners() {
        let notifier = PowerStateNotifierImpl::new(Arc::new(MockCommunicate::new()), "test");
        let report = PowerStateReport::ON;

        let result = notifier.notify(report).await;
        assert!(result.is_ok(), "notify failed when no listeners are present: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_on_client_died_removes_callback() {
        let callbacks_map = Arc::new(Mutex::new(HashMap::new()));
        let power_off_exit_state = Arc::new(PowerOffExit {});
        let (_tx, rx) =
            tokio::sync::watch::channel(power_off_exit_state.clone() as Arc<dyn PowerState>);
        let mut mock_death_linker = MockDeathLinker::default();
        mock_death_linker
            .expect_link_callback_to_death_recipient()
            .times(1)
            .returning(|_, _| Ok(()));
        let server = BinderServer::new(callbacks_map.clone(), rx, Arc::new(mock_death_linker));
        let fake_listener_impl = MockIPowerStateReportListener::new();
        let fake_listener_binder =
            BnPowerStateReportListener::new_binder(fake_listener_impl, BinderFeatures::default());

        let result = server.subscribeToPowerStateReport(&fake_listener_binder).await;
        assert!(result.is_ok(), "subscribeToPowerStateReport failed: {:?}", result.err());
        assert_eq!(callbacks_map.lock().unwrap().len(), 1);

        let id = {
            let binding = callbacks_map.lock().unwrap();
            let client_id_to_remove = binding.keys().next().unwrap();
            *client_id_to_remove
        };

        on_client_died(callbacks_map.clone(), id);

        assert_eq!(server.callbacks.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_notify_one_listener_success() {
        let notifier = PowerStateNotifierImpl::new(Arc::new(MockCommunicate::new()), "test");
        let report = PowerStateReport::ON;
        let report_aidl = convert_to_aidl(report).unwrap();

        let mock_listener = MockIPowerStateReportListener::new();
        let client_callback = create_mock_listener_callback(mock_listener, report_aidl, true);
        notifier.callbacks.lock().unwrap().insert(123u32, client_callback);

        let result = notifier.notify(report).await;
        assert!(result.is_ok(), "notify failed for one successful listener: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_notify_one_listener_fails() {
        let notifier = PowerStateNotifierImpl::new(Arc::new(MockCommunicate::new()), "test");
        let report = PowerStateReport::POWER_OFF_ENTER;
        let report_aidl = convert_to_aidl(report).unwrap();

        let mock_listener = MockIPowerStateReportListener::new();
        let client_callback = create_mock_listener_callback(mock_listener, report_aidl, false);
        notifier.callbacks.lock().unwrap().insert(456u32, client_callback);

        let result = notifier.notify(report).await;
        assert!(result.is_err(), "notify should have failed for one failing listener");
        assert!(matches!(result.unwrap_err(), PowerStateNotificationError::Failed));
    }

    #[tokio::test]
    async fn test_notify_multiple_listeners_all_succeed() {
        let notifier = PowerStateNotifierImpl::new(Arc::new(MockCommunicate::new()), "test");
        let report = PowerStateReport::SUSPEND_TO_RAM_ENTER;
        let report_aidl = convert_to_aidl(report).unwrap();

        let mock_listener1 = MockIPowerStateReportListener::new();
        let client_callback1 = create_mock_listener_callback(mock_listener1, report_aidl, true);
        notifier.callbacks.lock().unwrap().insert(1u32, client_callback1);

        let mock_listener2 = MockIPowerStateReportListener::new();
        let client_callback2 = create_mock_listener_callback(mock_listener2, report_aidl, true);
        notifier.callbacks.lock().unwrap().insert(2u32, client_callback2);

        let result = notifier.notify(report).await;
        assert!(
            result.is_ok(),
            "notify failed for multiple successful listeners: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_notify_multiple_listeners_one_fails() {
        let notifier = PowerStateNotifierImpl::new(Arc::new(MockCommunicate::new()), "test");
        let report = PowerStateReport::WAIT_FOR_FINISH;
        let report_aidl = convert_to_aidl(report).unwrap();

        let mock_listener_success = MockIPowerStateReportListener::new();
        let client_callback_success =
            create_mock_listener_callback(mock_listener_success, report_aidl, true);
        notifier.callbacks.lock().unwrap().insert(1u32, client_callback_success);

        let mock_listener_fail = MockIPowerStateReportListener::new();
        let client_callback_fail =
            create_mock_listener_callback(mock_listener_fail, report_aidl, false);
        notifier.callbacks.lock().unwrap().insert(2u32, client_callback_fail);

        let result = notifier.notify(report).await;
        assert!(result.is_err(), "notify should have failed when one of multiple listeners fails");
        assert!(matches!(result.unwrap_err(), PowerStateNotificationError::Failed));
    }

    #[tokio::test]
    async fn notify_oem_success_first_try() {
        let mut mock_comms = MockCommunicate::new();
        mock_comms.expect_create_client_transport().times(1).returning(|_| {
            let mut mock_client_transport = MockTestClientTransport::new();
            mock_client_transport.expect_call_method().times(1).returning(|_, _| Ok(Vec::new()));
            Box::pin(async move { Ok(Box::new(mock_client_transport) as Box<dyn ClientTransport>) })
        });

        let notifier = PowerStateNotifierImpl::new(Arc::new(mock_comms), "test_vm");
        assert!(notifier.update_client().await.is_ok());

        let result =
            notifier.notify_oem(PowerStateReport::ON, PowerStateChangeReason::HOST_REQUESTED).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn notify_oem_success_after_initial_client_update() {
        let mut mock_comms = MockCommunicate::new();
        mock_comms.expect_create_client_transport().times(1).returning(|_| {
            let mut mock_client_transport = MockTestClientTransport::new();
            mock_client_transport.expect_call_method().times(1).returning(|_, _| Ok(Vec::new()));
            Box::pin(async move { Ok(Box::new(mock_client_transport) as Box<dyn ClientTransport>) })
        });

        let notifier = PowerStateNotifierImpl::new(Arc::new(mock_comms), "test_vm");

        let result =
            notifier.notify_oem(PowerStateReport::ON, PowerStateChangeReason::HOST_REQUESTED).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn notify_oem_fails_if_initial_client_update_fails() {
        let mut mock_comms = MockCommunicate::new();
        mock_comms
            .expect_create_client_transport()
            .times(1)
            .returning(|_| Box::pin(async move { Err(SdvStatus::new(SdvStatusCode::Unknown)) }));

        let notifier = PowerStateNotifierImpl::new(Arc::new(mock_comms), "test_vm");

        let result =
            notifier.notify_oem(PowerStateReport::ON, PowerStateChangeReason::HOST_REQUESTED).await;
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "Could not get a client for vpm notifications"
        );
    }

    #[tokio::test]
    async fn notify_oem_success_after_retry_due_to_unavailable() {
        let mut mock_comms = MockCommunicate::new();
        mock_comms.expect_create_client_transport().times(1).returning(|_| {
            let mut mock_client_transport = MockTestClientTransport::new();
            mock_client_transport
                .expect_call_method()
                .times(1)
                .returning(|_, _| Err(SdvStatus::new(SdvStatusCode::Unavailable)));
            Box::pin(async move { Ok(Box::new(mock_client_transport) as Box<dyn ClientTransport>) })
        });
        mock_comms.expect_create_client_transport().times(1).returning(|_| {
            let mut mock_client_transport = MockTestClientTransport::new();
            mock_client_transport.expect_call_method().times(1).returning(|_, _| Ok(Vec::new()));
            Box::pin(async move { Ok(Box::new(mock_client_transport) as Box<dyn ClientTransport>) })
        });

        let notifier = PowerStateNotifierImpl::new(Arc::new(mock_comms), "test_vm");

        let result =
            notifier.notify_oem(PowerStateReport::ON, PowerStateChangeReason::HOST_REQUESTED).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn notify_oem_fails_if_retry_update_client_fails() {
        let mut mock_comms = MockCommunicate::new();
        mock_comms.expect_create_client_transport().times(1).returning(|_| {
            let mut mock_client_transport = MockTestClientTransport::new();
            mock_client_transport
                .expect_call_method()
                .times(1)
                .returning(|_, _| Err(SdvStatus::new(SdvStatusCode::Unavailable)));
            Box::pin(async move { Ok(Box::new(mock_client_transport) as Box<dyn ClientTransport>) })
        });
        mock_comms
            .expect_create_client_transport()
            .times(1)
            .returning(|_| Box::pin(async move { Err(SdvStatus::new(SdvStatusCode::Internal)) }));

        let notifier = PowerStateNotifierImpl::new(Arc::new(mock_comms), "test_vm");
        assert!(notifier.update_client().await.is_ok());

        let result =
            notifier.notify_oem(PowerStateReport::ON, PowerStateChangeReason::HOST_REQUESTED).await;
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "Could not get a client for vpm notifications"
        );
    }

    #[tokio::test]
    async fn notify_oem_fails_if_retry_notify_fails() {
        let mut mock_comms = MockCommunicate::new();
        mock_comms.expect_create_client_transport().times(1).returning(|_| {
            let mut mock_client_transport = MockTestClientTransport::new();
            mock_client_transport
                .expect_call_method()
                .times(1)
                .returning(|_, _| Err(SdvStatus::new(SdvStatusCode::Unavailable)));
            Box::pin(async move { Ok(Box::new(mock_client_transport) as Box<dyn ClientTransport>) })
        });
        mock_comms.expect_create_client_transport().times(1).returning(|_| {
            let mut mock_client_transport = MockTestClientTransport::new();
            mock_client_transport
                .expect_call_method()
                .times(1)
                .returning(|_, _| Err(SdvStatus::new(SdvStatusCode::DeadlineExceeded)));
            Box::pin(async move { Ok(Box::new(mock_client_transport) as Box<dyn ClientTransport>) })
        });

        let notifier = PowerStateNotifierImpl::new(Arc::new(mock_comms), "test_vm");
        assert!(notifier.update_client().await.is_ok());

        let result =
            notifier.notify_oem(PowerStateReport::ON, PowerStateChangeReason::HOST_REQUESTED).await;
        assert!(result.is_err());
        let err_msg = result.err().unwrap().to_string();
        assert!(err_msg.starts_with("Request to notify VPM client failed"));
        assert!(err_msg.contains("DeadlineExceeded"));
    }

    #[tokio::test]
    async fn notify_oem_fails_non_unavailable_error_no_retry() {
        let mut mock_comms = MockCommunicate::new();
        mock_comms.expect_create_client_transport().times(1).returning(|_| {
            let mut mock_client_transport = MockTestClientTransport::new();
            mock_client_transport
                .expect_call_method()
                .times(1)
                .returning(|_, _| Err(SdvStatus::new(SdvStatusCode::InvalidArgument)));
            Box::pin(async move { Ok(Box::new(mock_client_transport) as Box<dyn ClientTransport>) })
        });

        let notifier = PowerStateNotifierImpl::new(Arc::new(mock_comms), "test_vm");
        assert!(notifier.update_client().await.is_ok());

        let result =
            notifier.notify_oem(PowerStateReport::ON, PowerStateChangeReason::HOST_REQUESTED).await;
        assert!(result.is_err());
        let err_msg = result.err().unwrap().to_string();
        assert!(err_msg.starts_with("Request to notify VPM client failed"));
        assert!(err_msg.contains("InvalidArgument"));
    }

    fn create_mock_listener_callback(
        mut mock_listener: MockIPowerStateReportListener,
        expected_report_aidl: PowerStateReportAidl,
        should_succeed: bool,
    ) -> ClientCallback {
        if should_succeed {
            mock_listener
                .expect_onPowerStateReportChanged()
                .withf(move |report_arg| *report_arg == expected_report_aidl)
                .times(1)
                .returning(|_| Ok(()));
        } else {
            mock_listener
                .expect_onPowerStateReportChanged()
                .withf(move |report_arg| *report_arg == expected_report_aidl)
                .times(1)
                .returning(|_| Err(Status::new_exception(ExceptionCode::TRANSACTION_FAILED, None)));
        }

        let listener_binder =
            BnPowerStateReportListener::new_binder(mock_listener, BinderFeatures::default());
        ClientCallback {
            async_callback: listener_binder.into_async(),
            death_recipient: DeathRecipient::new(|| {}), // Dummy for this test's scope
        }
    }

    #[tokio::test]
    async fn test_create_binder_service_if_add_service() {
        let mock_comms = Arc::new(MockCommunicate::new());
        let notifier = PowerStateNotifierImpl::new(mock_comms, "test_vm_binder_error");

        let initial_state = Arc::new(PowerOffExit {}) as Arc<dyn PowerState>;
        let (_tx, rx) = tokio::sync::watch::channel(initial_state);

        let result = notifier.create_binder_service(rx.clone());
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handle_is_err_due_to_cancellation() {
        let mut set = JoinSet::new();

        // Spawn a task that will run indefinitely until explicitly cancelled.
        // We capture its JoinHandle to abort it later.
        let task_handle = set.spawn(async { std::future::pending().await });

        // Immediately abort the spawned task.
        // This will cause `task_handle` to resolve to an `Err(JoinError::Cancelled)`
        // when `set.join_next().await` is called in `notification_failed`.
        task_handle.abort();

        // Call the function under test.
        // Your function should now correctly detect the `JoinError::Cancelled`
        // via `handle.is_err()`.
        let failed = notification_failed(set).await;

        // Assert that `notification_failed` returned `true`,
        // indicating it successfully handled the task cancellation.
        assert!(failed);
    }
}
