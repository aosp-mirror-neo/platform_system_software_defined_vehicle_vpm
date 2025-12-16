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

mod power_service;
mod power_state;
mod power_state_notifier;
mod system_service;
use crate::power_service::PowerService;
use crate::power_service::PowerServiceImpl;
use crate::power_service::PowerStateRequestHandlerImpl;
use crate::power_state::PowerState;
use crate::power_state_notifier::PowerStateNotifier;
use crate::power_state_notifier::PowerStateNotifierImpl;
use crate::system_service::VpmService;
use crate::system_service::VpmServiceImpl;
use anyhow::anyhow;
use log::error;
use log::info;
use mockall::automock;
use sdv_comms::id::ServiceFqin;
use sdv_comms::ContextRef;
use sdv_mw_rs_com_sdv_google_vpm_vpm_system_service_bundle::publisher::Variant;
use sdv_mw_rs_com_sdv_google_vpm_vpm_system_service_bundle::VpmSystemServiceBundle;
use sdv_mw_rs_comms::Publisher;
use sdv_mw_rs_sdv_comms::SdvComms;
use sdv_vpm_api::vehicle::VehicleStateChange;
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use tracing::instrument;

const POWER_STATE_CHANNEL_QUEUE_SIZE: usize = 1;

const FQIN: &str = "vm:com.sdv.google.vpm.VpmSystemServiceBundle/default";

/// VPM agent.
/// The agent contains service implementation for vehicle and power mode management.
pub struct Agent {
    system_service: Box<dyn VpmService>,
    power_service: Box<dyn PowerService>,
    vpm_bundle: Box<dyn VpmServiceBundle>,
}

#[automock]
trait VpmServiceBundle: Send {
    fn get_vehicle_state_publisher(&mut self) -> Option<Publisher<VehicleStateChange>>;
}

impl VpmServiceBundle for VpmSystemServiceBundle {
    fn get_vehicle_state_publisher(&mut self) -> Option<Publisher<VehicleStateChange>> {
        self.take_publisher(Variant::UNIQUE)
    }
}

/// VPM agent implementation.
impl Agent {
    /// Creates a new VPM agent using the default FQIN.
    #[instrument]
    pub async fn new() -> anyhow::Result<Self> {
        Self::new_with_fqin(FQIN).await
    }

    /// Creates a new VPM agent with a specific FQIN.
    /// This function is crate-public for testing purposes.
    #[instrument]
    async fn new_with_fqin(fqin: &str) -> anyhow::Result<Self> {
        let service_fqin = ServiceFqin::from_str(fqin)
            .map_err(|e| anyhow!("Could not parse FQIN for VPM: {e}"))?;
        let context = ContextRef::create(service_fqin.clone());
        let sdv_comms = Arc::new(SdvComms { context });
        let self_fqin = sdv_comms.context.get_self_fqin();
        let vm_name = self_fqin.get_sdv_vm_name();
        Self::new_with_comms(sdv_comms, vm_name).await
    }

    /// Creates a new VPM agent with a specific comms implementation.
    async fn new_with_comms(
        comms: Arc<dyn sdv_mw_rs_comms::Communicate>,
        vm_name: &str,
    ) -> anyhow::Result<Self> {
        let power_service = Box::new(PowerServiceImpl::new(comms.clone(), vm_name)?);
        let (system_service, rpc_server) = VpmServiceImpl::new(
            power_service.get_power_state_request_sender(),
            power_service.get_power_state_watch_receiver(),
        );
        let vpm_bundle = VpmSystemServiceBundle::new(comms, (Arc::new(rpc_server),))
            .await
            .map_err(|e| anyhow!("VPM Bundle failed to initialize {e}"))?;
        Ok(Self::new_with_services(Box::new(system_service), power_service, Box::new(vpm_bundle)))
    }

    fn new_with_services(
        system_service: Box<dyn VpmService>,
        power_service: Box<dyn PowerService>,
        vpm_bundle: Box<dyn VpmServiceBundle>,
    ) -> Self {
        Self { system_service, power_service, vpm_bundle }
    }

    /// Starts the VPM agent.
    /// The agent will start the vehicle state service and the power mode state service.
    pub async fn start(&mut self) {
        info!("Starting VPM Agent.");
        let _ = tokio::join!(
            self.system_service.start(self.vpm_bundle.get_vehicle_state_publisher()),
            self.power_service.start()
        );
    }
}

/// Spawns a tokio thread that runs the agent.
pub async fn create_and_start_agent<F, Fut>(agent_factory: F) -> anyhow::Result<()>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = anyhow::Result<Agent>>,
{
    let mut agent = agent_factory().await.map_err(|e| {
        error!("Failed to create VPM Agent: {e}");
        e
    })?;

    tokio::spawn(async move {
        agent.start().await;
    });

    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::power_service::MockPowerService;
    use crate::power_service::MockPowerStateRequestHandler;
    use crate::power_service::PowerRequest;
    use crate::power_state::PowerOffExit;
    use crate::system_service::MockVpmService;
    use crate::system_service::RpcServer;
    use crate::Agent;
    use sdv_mw_rs_comms::MockPublish;
    use sdv_mw_rs_comms::{MockCommunicate, ServerTransport};
    use sdv_status_rs::{SdvStatus, SdvStatusCode};
    use sdv_vpm_api::vehicle::VpmVehicleState;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::mpsc::channel;
    use tokio::sync::watch::channel as WatchChannel;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn vpm_agent_starts() {
        // Given: a mock services.
        let mut mock_vpm_service = MockVpmService::default();
        let mut mock_power_service = MockPowerService::default();
        let mut mock_vpm_bundle = MockVpmServiceBundle::default();
        let (tx, _) = channel::<PowerRequest>(1);
        let (_, rx) = WatchChannel::<Arc<dyn PowerState>>(Arc::new(PowerOffExit {}));

        mock_power_service
            .expect_get_power_state_request_sender()
            .times(1)
            .returning(move || tx.clone());

        mock_power_service
            .expect_get_power_state_watch_receiver()
            .times(1)
            .returning(move || rx.clone());

        // Expect: the publisher and the rpc server will be started.return_const(()).
        mock_vpm_service.expect_start().times(1).return_const(());
        // Expect: the power service to start.
        mock_power_service.expect_start().times(1).returning(|| Ok(()));
        // Expect: get vehicle state publisher to be called.
        mock_vpm_bundle.expect_get_vehicle_state_publisher().times(1).returning(|| None);

        // When: the new agent is created, publisher and rpc servers are created.
        let mut agent = Agent::new_with_services(
            Box::new(mock_vpm_service),
            Box::new(mock_power_service),
            Box::new(mock_vpm_bundle),
        );

        // Then: assert power state request sender and watch receiver are accessible.
        let _sender = agent.power_service.get_power_state_request_sender();
        let _watch_receiver = agent.power_service.get_power_state_watch_receiver();

        // Then: assert agent can start.
        let _test = agent.start().await;
    }

    #[tokio::test]
    async fn vpm_agent_starts_with_publisher() {
        // Given: mock services.
        let mut mock_vpm_service = MockVpmService::new();
        let mut mock_power_service = MockPowerService::new();
        let mut mock_vpm_bundle = MockVpmServiceBundle::new();
        let (tx, _) = channel::<PowerRequest>(1);
        let (_, rx) = WatchChannel::<Arc<dyn PowerState>>(Arc::new(PowerOffExit {}));

        let mock_publisher_handle = Box::new(MockPublish::new());
        let publisher = Publisher::<VehicleStateChange>::new(mock_publisher_handle);

        mock_power_service.expect_get_power_state_request_sender().returning(move || tx.clone());

        mock_power_service.expect_get_power_state_watch_receiver().returning(move || rx.clone());

        // Expect: the vpm bundle to provide a publisher.
        mock_vpm_bundle
            .expect_get_vehicle_state_publisher()
            .times(1)
            .return_once(|| Some(publisher));

        // Expect: the system service to be started with the publisher.
        mock_vpm_service.expect_start().withf(|p| p.is_some()).times(1).return_const(());

        // Expect: the power service to start.
        mock_power_service.expect_start().times(1).returning(|| Ok(()));

        // When: the new agent is created
        let mut agent = Agent::new_with_services(
            Box::new(mock_vpm_service),
            Box::new(mock_power_service),
            Box::new(mock_vpm_bundle),
        );

        // Then: agent can start.
        agent.start().await;
    }

    #[tokio::test]
    async fn get_vehicle_state_publisher_takes_publisher_on_successive_calls() {
        // This test validates that `get_vehicle_state_publisher` correctly takes ownership
        // of the publisher, returning None on subsequent calls.

        // Given: A VpmSystemServiceBundle initialized successfully.
        let mut mock_comms = MockCommunicate::new();
        let (tx, _) = channel::<VpmVehicleState>(1);
        let mock_power_handler = MockPowerStateRequestHandler::new();
        let rpc_server = Arc::new(RpcServer::new(tx, Box::new(mock_power_handler)));
        struct MockServerTransport;
        impl ServerTransport for MockServerTransport {}

        mock_comms.expect_create_publish().times(1).returning(|_| {
            let mock_publish_handle: Box<dyn sdv_mw_rs_comms::Publish> =
                Box::new(MockPublish::new());
            Box::pin(async move { Ok(mock_publish_handle) })
        });
        mock_comms.expect_create_server_transport_all_methods().times(1).returning(|_, _| {
            let mock_server_transport: Box<dyn ServerTransport> = Box::new(MockServerTransport);
            Box::pin(async move { Ok(mock_server_transport) })
        });
        let bundle =
            VpmSystemServiceBundle::new(Arc::new(mock_comms), (rpc_server,)).await.unwrap();
        let mut bundle: Box<dyn VpmServiceBundle> = Box::new(bundle);

        // When: get_vehicle_state_publisher is called for the first time.
        let first_call_result = bundle.get_vehicle_state_publisher();

        // Then: it returns a publisher.
        assert!(first_call_result.is_some(), "Expected to get a publisher on the first call.");

        // When: get_vehicle_state_publisher is called for the second time.
        let second_call_result = bundle.get_vehicle_state_publisher();

        // Then: it returns None, as the publisher has been taken.
        assert!(second_call_result.is_none(), "Expected to get None on the second call.");
    }

    #[tokio::test]
    async fn new_with_fqin_fails_with_invalid_fqin() {
        // When Agent::new_with_fqin is called with an invalid FQIN.
        let result = Agent::new_with_fqin("invalid fqin").await;

        // Then it should fail with an error message.
        assert!(result.is_err());
        assert!(!result.err().unwrap().to_string().is_empty());
    }

    #[tokio::test]
    async fn new_with_comms_fails_when_bundle_fails() {
        // Given a mock comms that fails to create a publisher.
        let mut mock_comms = MockCommunicate::new();
        mock_comms
            .expect_create_publish()
            .times(1)
            .returning(|_| Box::pin(async { Err(SdvStatus::new(SdvStatusCode::Unknown)) }));

        // When Agent::new_with_comms is called.
        let result = Agent::new_with_comms(Arc::new(mock_comms), "test_vm").await;

        // Then it should fail with a specific error message.
        assert!(result.is_err());
        assert!(!result.err().unwrap().to_string().is_empty());
    }

    #[tokio::test]
    async fn new_with_comms_succeeds() {
        // Given a mock comms that succeeds in creating publishers and servers.
        let mut mock_comms = MockCommunicate::new();

        // Mock for VpmSystemServiceBundle::new -> fill_publication_variants
        mock_comms.expect_create_publish().times(1).returning(|_| {
            let mock_publish_handle: Box<dyn sdv_mw_rs_comms::Publish> =
                Box::new(MockPublish::new());
            Box::pin(async move { Ok(mock_publish_handle) })
        });

        // Mock for VpmSystemServiceBundle::new -> add_server
        struct MockServerTransport;
        impl ServerTransport for MockServerTransport {}
        mock_comms.expect_create_server_transport_all_methods().times(1).returning(|_, _| {
            let mock_server_transport: Box<dyn ServerTransport> = Box::new(MockServerTransport);
            Box::pin(async move { Ok(mock_server_transport) })
        });

        // When Agent::new_with_comms is called.
        let result = Agent::new_with_comms(Arc::new(mock_comms), "test_vm").await;

        // Then it should succeed.
        assert!(result.is_ok(), "Agent::new_with_comms failed: {:?}", result.err());
    }

    #[tokio::test]
    async fn create_and_start_agent_handles_error() {
        // Given a mock comms that fails to create a publisher, which will cause Agent::new to fail.
        let mut mock_comms = MockCommunicate::new();
        mock_comms
            .expect_create_publish()
            .times(1)
            .returning(|_| Box::pin(async { Err(SdvStatus::new(SdvStatusCode::Unknown)) }));

        // When create_and_start_agent is called with a factory that uses the failing comms.
        let result =
            create_and_start_agent(|| Agent::new_with_comms(Arc::new(mock_comms), "test_vm")).await;

        // Then it should fail.
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn create_and_start_agent_succeeds() {
        // Given mock services and an atomic bool to track if start is called.
        let mut mock_vpm_service = MockVpmService::new();
        let mut mock_power_service = MockPowerService::new();
        let mut mock_vpm_bundle = MockVpmServiceBundle::new();
        let start_called = Arc::new(AtomicBool::new(false));
        let start_called_clone = start_called.clone();

        // Expect start to be called on the services, and set the flag.
        mock_vpm_service.expect_start().times(1).return_const(());
        mock_power_service.expect_start().times(1).returning(move || {
            start_called_clone.store(true, Ordering::Relaxed);
            Ok(())
        });
        mock_vpm_bundle.expect_get_vehicle_state_publisher().times(1).returning(|| None);

        // When create_and_start_agent is called with a factory for the mock agent.
        let agent_factory = || async {
            Ok(Agent::new_with_services(
                Box::new(mock_vpm_service),
                Box::new(mock_power_service),
                Box::new(mock_vpm_bundle),
            ))
        };
        let result = create_and_start_agent(agent_factory).await;

        // Then the function should succeed and the agent's start method should be called.
        assert!(result.is_ok());
        let start_result = timeout(Duration::from_secs(3), async {
            while !start_called.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(start_result.is_ok(), "Timeout waiting for start to be called");
    }
}
