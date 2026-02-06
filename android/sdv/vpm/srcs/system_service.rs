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
//! Vehicle and Power Mode Agent.
//!
use crate::power_service::PowerRequest;
use crate::power_service::PowerStateRequestHandler;
use crate::PowerState;
use crate::PowerStateRequestHandlerImpl;
use async_trait::async_trait;
use com_android_sdv_vpm_vpm_system_service_rpc::Interface;
use log::{error, info};
use mockall::automock;
use sdv_mw_rs_comms::Publisher;
use sdv_status_rs::SdvResult;
use sdv_status_rs::SdvStatus;
use sdv_status_rs::SdvStatusCode;
use sdv_vpm_api::vehicle::VehicleStateChange;
use sdv_vpm_api::vehicle::VpmVehicleState;
use sdv_vpm_api::vpm::power_state_change_response::PowerStateChangeRequestError;
use sdv_vpm_api::vpm::GetPowerStateRequest;
use sdv_vpm_api::vpm::GetPowerStateResponse;
use sdv_vpm_api::vpm::PowerStateChangeRequest;
use sdv_vpm_api::vpm::PowerStateChangeResponse;
use sdv_vpm_api::vpm::SetVehicleStateRequest;
use sdv_vpm_api::vpm::SetVehicleStateResponse;
use std::sync::Arc;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender as MpscSender;
use tokio::sync::watch::Receiver as WatchReceiver;
use tracing::instrument;

const VEHICLE_STATE_CHANNEL_QUEUE_SIZE: usize = 1;

#[automock]
#[async_trait]
// The trait encapsulates the functions of the service bundle.
// It also helps with testing the VPM agent while mocking the middleware generated code.
pub trait VpmService: Send + Sync {
    // Starts the rpc server that receives incoming RPC requests from the OEM to set the vehicle and power state in the SDV VM.
    async fn start(&mut self, vehicle_state_publisher: Option<Publisher<VehicleStateChange>>);
}

pub struct VpmServiceImpl {
    vehicle_state_receiver: Receiver<VpmVehicleState>,
}

impl VpmServiceImpl {
    // Returns the new vpm service and the RPC server used for handling the RPC calls.
    pub fn new(
        power_state_request_sender: MpscSender<PowerRequest>,
        power_state_watch_receiver: WatchReceiver<Arc<dyn PowerState>>,
    ) -> (Self, RpcServer) {
        let (vehicle_state_sender, vehicle_state_receiver) =
            channel(VEHICLE_STATE_CHANNEL_QUEUE_SIZE);
        let power_state_request_handler = Box::new(PowerStateRequestHandlerImpl::new(
            power_state_request_sender,
            power_state_watch_receiver,
        ));
        (
            Self { vehicle_state_receiver },
            RpcServer::new(vehicle_state_sender, power_state_request_handler),
        )
    }
}

#[async_trait]
impl VpmService for VpmServiceImpl {
    async fn start(&mut self, vehicle_state_publisher: Option<Publisher<VehicleStateChange>>) {
        let Some(vehicle_state_publisher) = vehicle_state_publisher else {
            error!("Vehicle state publisher could not be started!");
            return;
        };

        while let Some(vehicle_state) = self.vehicle_state_receiver.recv().await {
            let mut vehicle_state_change_message = VehicleStateChange::new();
            vehicle_state_change_message.vehicle_state = vehicle_state.into();
            info!("Publishing Vpm vehicle mode, {vehicle_state:?}!");
            let result = vehicle_state_publisher.publish(&vehicle_state_change_message);
            if result.is_err() {
                error!("Failed to publish vehicle state {vehicle_state:?}");
            }
        }
    }
}

/// A listener struct that handles incoming RPC requests for setting the vpm.
pub struct RpcServer {
    vehicle_state_sender: MpscSender<VpmVehicleState>,
    power_state_request_handler: Box<dyn PowerStateRequestHandler>,
}

impl RpcServer {
    pub fn new(
        vehicle_state_sender: MpscSender<VpmVehicleState>,
        power_state_request_handler: Box<dyn PowerStateRequestHandler>,
    ) -> Self {
        Self { vehicle_state_sender, power_state_request_handler }
    }
}

/// Middleware RPC server trait implementation for RPC server.
#[allow(non_snake_case, missing_docs)]
#[async_trait::async_trait]
impl Interface for RpcServer {
    #[instrument(skip(self))]
    async fn SetVehicleState(
        &self,
        _caller_id: sdv_comms::id::ServiceFqin,
        request: &SetVehicleStateRequest,
    ) -> SdvResult<SetVehicleStateResponse> {
        info!("Received Vehicle state change request! {}", request);
        let state = request.vehicle_state.enum_value_or_default();
        if state == VpmVehicleState::VEHICLE_STATE_UNSPECIFIED {
            return Err(SdvStatus::with_message(
                SdvStatusCode::Internal,
                "Vehicle state was not set!".to_string(),
            ));
        }
        let result = self.vehicle_state_sender.send(state).await;
        if result.is_err() {
            return Err(SdvStatus::with_message(
                SdvStatusCode::Internal,
                "Could not publish vehicle state!".to_string(),
            ));
        }
        Ok(SetVehicleStateResponse::new())
    }

    #[instrument(skip(self))]
    async fn SetPowerState(
        &self,
        _caller_id: sdv_comms::id::ServiceFqin,
        request: &PowerStateChangeRequest,
    ) -> SdvResult<PowerStateChangeResponse> {
        let result = self.power_state_request_handler.handle_set_power_state(request.clone()).await;
        if result.is_ok() {
            let mut response = PowerStateChangeResponse::new();
            response.error = PowerStateChangeRequestError::NO_ERROR.into();
            Ok(response)
        } else {
            let mut response = PowerStateChangeResponse::new();
            response.error = PowerStateChangeRequestError::INVALID_TRANSITION.into();
            Ok(response)
        }
    }

    #[instrument(skip(self))]
    async fn GetPowerState(
        &self,
        _caller_id: sdv_comms::id::ServiceFqin,
        _request: &GetPowerStateRequest,
    ) -> SdvResult<GetPowerStateResponse> {
        let power_state = self.power_state_request_handler.get_current_power_state().await;
        let mut response = GetPowerStateResponse::new();
        response.power_state = power_state.into();
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::power_service::MockPowerStateRequestHandler;
    use crate::power_state::On;
    use anyhow::anyhow;
    use com_android_sdv_vpm_vpm_system_service_rpc::Interface;
    use sdv_comms::id::ServiceFqin;
    use sdv_mw_rs_comms::codec::decode;
    use sdv_mw_rs_comms::MockPublish;
    use sdv_mw_rs_comms::Publisher;
    use sdv_status_rs::SdvStatus;
    use sdv_status_rs::SdvStatusCode;
    use sdv_vpm_api::power::PowerStateReport;
    use sdv_vpm_api::vehicle::VpmVehicleState;
    use sdv_vpm_api::vpm::power_state_change_response::PowerStateChangeRequestError;
    use sdv_vpm_api::vpm::GetPowerStateRequest;
    use sdv_vpm_api::vpm::PowerStateChangeRequest;
    use sdv_vpm_api::vpm::PowerStateRequest;
    use sdv_vpm_api::vpm::SetVehicleStateRequest;
    use sdv_vpm_api::vpm::ShutdownType;
    use std::str::FromStr;
    use std::time::Duration;
    use tokio::sync::mpsc::channel;
    use tokio::time::timeout;

    #[tokio::test]
    async fn set_rpc_vehicle_state() {
        // Given: mock services.
        let (vehicle_state_sender, mut vehicle_state_receiver) =
            channel(VEHICLE_STATE_CHANNEL_QUEUE_SIZE);
        let mock_power_state_request_handler = MockPowerStateRequestHandler::default();
        let rpc_server =
            RpcServer::new(vehicle_state_sender, Box::new(mock_power_state_request_handler));
        let vehicle_state_handle = tokio::spawn(async move { vehicle_state_receiver.recv().await });

        // When: a vehicle state request is sent
        let request = SetVehicleStateRequest {
            vehicle_state: VpmVehicleState::VEHICLE_ON.into(),
            ..Default::default()
        };
        let fqin = ServiceFqin::from_str("vm:com.android.sdv.vpm.VpmSystemServiceBundle/default")
            .expect("Could not create context for tests!");
        let result = rpc_server.SetVehicleState(fqin, &request).await;

        // Then: vehicle state should be received by the channel.
        let expected_vehicle_state = vehicle_state_handle.await.unwrap();
        assert_eq!(
            VpmVehicleState::VEHICLE_ON,
            expected_vehicle_state.expect("Vehicle state should be sent over the channel")
        );
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn set_rpc_vehicle_state_fails_request_invalid() {
        // Given: mock services.
        let (vehicle_state_sender, _) = channel(VEHICLE_STATE_CHANNEL_QUEUE_SIZE);
        let mock_power_state_request_handler = MockPowerStateRequestHandler::default();
        let rpc_server =
            RpcServer::new(vehicle_state_sender, Box::new(mock_power_state_request_handler));

        // When: a vehicle state request is sent without proper state.
        let request = SetVehicleStateRequest { ..Default::default() };

        // Then: result should be error.
        let fqin = ServiceFqin::from_str("vm:com.android.sdv.vpm.VpmSystemServiceBundle/default")
            .expect("Could not create context for tests!");
        let err = rpc_server.SetVehicleState(fqin, &request).await.err().unwrap();
        assert_eq!(err.status_code(), SdvStatusCode::Internal);
    }

    #[tokio::test]
    async fn set_rpc_vehicle_state_fails_no_receiver() {
        // Given: mock services without a receiver.
        let (vehicle_state_sender, vehicle_state_receiver) =
            channel(VEHICLE_STATE_CHANNEL_QUEUE_SIZE);
        let mock_power_state_request_handler = MockPowerStateRequestHandler::default();
        let rpc_server =
            RpcServer::new(vehicle_state_sender, Box::new(mock_power_state_request_handler));

        // When: a vehicle state request is sent and receiver is dropped.
        let request = SetVehicleStateRequest {
            vehicle_state: VpmVehicleState::VEHICLE_ON.into(),
            ..Default::default()
        };
        drop(vehicle_state_receiver);

        // Then: result should be error.
        let fqin = ServiceFqin::from_str("vm:com.android.sdv.vpm.VpmSystemServiceBundle/default")
            .expect("Could not create context for tests!");
        let err = rpc_server.SetVehicleState(fqin, &request).await.err().unwrap();
        assert_eq!(err.status_code(), SdvStatusCode::Internal);
    }

    #[tokio::test]
    async fn set_rpc_power_state_succeeds() {
        // Given: a mock power service.
        let (vehicle_state_sender, _) = channel(VEHICLE_STATE_CHANNEL_QUEUE_SIZE);
        let mut mock_power_state_request_handler = MockPowerStateRequestHandler::default();
        mock_power_state_request_handler
            .expect_handle_set_power_state()
            .times(1)
            .returning(|_| Ok(()));
        let rpc_server =
            RpcServer::new(vehicle_state_sender, Box::new(mock_power_state_request_handler));

        // When: a power state request is sent
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::FINISH_SHUTDOWN.into(),
            shutdown_type: ShutdownType::POWER_OFF.into(),
            ..Default::default()
        };
        let fqin = ServiceFqin::from_str("vm:com.android.sdv.vpm.VpmSystemServiceBundle/default")
            .expect("Could not create context for tests!");
        let response = rpc_server.SetPowerState(fqin, &request).await;

        // Then: power state should be set in the power service.
        assert_eq!(response.unwrap().error, PowerStateChangeRequestError::NO_ERROR.into());
    }

    #[tokio::test]
    async fn get_rpc_power_state_succeeds() {
        // Given: a mock power service.
        let power_state = PowerStateReport::ON;
        let (vehicle_state_sender, _) = channel(VEHICLE_STATE_CHANNEL_QUEUE_SIZE);
        let mut mock_power_state_request_handler = MockPowerStateRequestHandler::default();
        mock_power_state_request_handler
            .expect_get_current_power_state()
            .times(1)
            .returning(move || power_state);
        let rpc_server =
            RpcServer::new(vehicle_state_sender, Box::new(mock_power_state_request_handler));

        // When: a power state request is sent
        let request = GetPowerStateRequest { ..Default::default() };
        let fqin = ServiceFqin::from_str("vm:com.android.sdv.vpm.VpmSystemServiceBundle/default")
            .expect("Could not create context for tests!");
        let response = rpc_server.GetPowerState(fqin, &request).await;

        // Then: power state should be returned.
        assert_eq!(response.unwrap().power_state.unwrap(), power_state);
    }

    #[tokio::test]
    async fn set_rpc_power_state_fails() {
        // Given: a mock power service.
        let (vehicle_state_sender, _) = channel(VEHICLE_STATE_CHANNEL_QUEUE_SIZE);
        let mut mock_power_state_request_handler = MockPowerStateRequestHandler::default();
        mock_power_state_request_handler
            .expect_handle_set_power_state()
            .times(1)
            .returning(|_| Err(anyhow!("Power service failed")));
        let rpc_server =
            RpcServer::new(vehicle_state_sender, Box::new(mock_power_state_request_handler));

        // When: a power state request is sent but fails.
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::FINISH_SHUTDOWN.into(),
            shutdown_type: ShutdownType::POWER_OFF.into(),
            ..Default::default()
        };
        let fqin = ServiceFqin::from_str("vm:com.android.sdv.vpm.VpmSystemServiceBundle/default")
            .expect("Could not create context for tests!");
        let response = rpc_server.SetPowerState(fqin, &request).await;

        // Then: power state should not be set in the power service.
        // Failure is reported.
        assert_eq!(
            response.unwrap().error,
            PowerStateChangeRequestError::INVALID_TRANSITION.into()
        );
    }

    #[tokio::test]
    async fn new_vpm_service_impl_creates_linked_components() {
        // Given
        let (power_req_tx, _power_req_rx) = channel::<PowerRequest>(1);
        let (_power_state_tx, power_state_rx) =
            tokio::sync::watch::channel(Arc::new(On {}) as Arc<dyn PowerState>);

        // When
        let (mut vpm_service, rpc_server) = VpmServiceImpl::new(power_req_tx, power_state_rx);

        // Then the RpcServer's sender is connected to the VpmServiceImpl's receiver.
        let state_to_send = VpmVehicleState::VEHICLE_ON;
        rpc_server.vehicle_state_sender.send(state_to_send).await.unwrap();

        let received_state = vpm_service.vehicle_state_receiver.recv().await.unwrap();
        assert_eq!(received_state, state_to_send);
    }

    #[tokio::test]
    async fn vpm_service_start_with_none_publisher_returns_quickly() {
        let (_sender, receiver) = channel(1);
        let mut service = VpmServiceImpl { vehicle_state_receiver: receiver };

        let result =
            tokio::time::timeout(tokio::time::Duration::from_millis(1000), service.start(None))
                .await;
        assert!(result.is_ok(), "start() with None publisher should not hang");
    }

    #[tokio::test]
    async fn vpm_service_start_publishes_vehicle_state() {
        // Given a VpmServiceImpl and a mock publisher
        let (sender, receiver) = channel(1);
        let mut service = VpmServiceImpl { vehicle_state_receiver: receiver };

        let mut mock_publish = MockPublish::new();
        let sent_state = VpmVehicleState::VEHICLE_ON;
        mock_publish
            .expect_publish_messages()
            .withf(move |messages| {
                if messages.len() != 1 {
                    return false;
                }
                let msg: VehicleStateChange = decode(&messages[0]).unwrap();
                msg.vehicle_state.enum_value_or_default() == sent_state
            })
            .times(1)
            .returning(|_| Ok(()));

        let publisher = Publisher::<VehicleStateChange>::new(Box::new(mock_publish));

        let service_handle = tokio::spawn(async move {
            service.start(Some(publisher)).await;
        });

        // When a vehicle state is sent to the service
        sender.send(sent_state).await.unwrap();
        drop(sender); // Close channel to terminate the loop in start()

        // Then the service should publish the state
        let result = timeout(Duration::from_secs(1), service_handle).await;
        assert!(result.is_ok(), "service.start() timed out");
    }

    #[tokio::test]
    async fn vpm_service_start_handles_publish_error() {
        // Given a service that will receive a message and a publisher that always fails
        let (sender, receiver) = channel(1);
        let mut service = VpmServiceImpl { vehicle_state_receiver: receiver };

        let mut mock_publish = MockPublish::new();
        mock_publish
            .expect_publish_messages()
            .times(1)
            .returning(|_| Err(SdvStatus::with_message(SdvStatusCode::Unknown, "publish failed")));
        let publisher = Publisher::<VehicleStateChange>::new(Box::new(mock_publish));
        let service_handle = tokio::spawn(async move { service.start(Some(publisher)).await });

        // When a message is sent
        sender.send(VpmVehicleState::VEHICLE_ON).await.unwrap();
        drop(sender);

        // Then the service should handle the errors gracefully and not panic
        let result = timeout(Duration::from_secs(1), service_handle).await;
        assert!(result.is_ok(), "service.start() timed out or panicked");
    }
}
