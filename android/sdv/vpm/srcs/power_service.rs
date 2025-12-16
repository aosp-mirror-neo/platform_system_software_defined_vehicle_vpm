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
use crate::power_state::*;
use crate::power_state_notifier::PowerStateNotificationError;
use crate::PowerStateNotifier;
use crate::PowerStateNotifierImpl;
use crate::POWER_STATE_CHANNEL_QUEUE_SIZE;
use anyhow::anyhow;
use async_trait::async_trait;
use log::{debug, error, info};
use mockall::automock;
use sdv_vpm_api::power::PowerStateReport;
use sdv_vpm_api::vpm::PowerStateChangeRequest;
use sdv_vpm_api::vpm::PowerStateRequest;
use sdv_vpm_api::vpm::ShutdownType;
use sdv_vpm_api::vpm_notifications::power_state_notification_request::PowerStateChangeReason;
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::watch::{
    channel as watch_channel, Receiver as WatchReceiver, Sender as WatchSender,
};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use tracing::Level;

#[automock]
#[async_trait]
pub trait PowerService: Send + Sync {
    async fn start(&mut self) -> anyhow::Result<()>;
    fn get_power_state_request_sender(&self) -> Sender<PowerRequest>;
    fn get_power_state_watch_receiver(&self) -> WatchReceiver<Arc<dyn PowerState>>;
}

struct PowerStateTracker {
    cancel_token: CancellationToken,
    power_state_handle: Option<JoinHandle<anyhow::Result<()>>>,
    power_report: Option<PowerStateReport>,
}

impl PowerStateTracker {
    fn new() -> Self {
        Self {
            cancel_token: CancellationToken::new(),
            power_state_handle: None,
            power_report: None,
        }
    }

    fn set_power_state_handle(
        &mut self,
        handle: JoinHandle<anyhow::Result<()>>,
        power_report: PowerStateReport,
    ) {
        self.power_state_handle = Some(handle);
        self.power_report = Some(power_report);
    }
}

pub struct PowerServiceImpl {
    power_state_notifier: Arc<dyn PowerStateNotifier>,
    power_state_request_sender: Sender<PowerRequest>,
    power_state_request_receiver: Receiver<PowerRequest>,
    power_state_watch_sender: Arc<WatchSender<Arc<dyn PowerState>>>,
    power_state_watch_receiver: WatchReceiver<Arc<dyn PowerState>>,
    power_state_tracker: PowerStateTracker,
}

impl PowerServiceImpl {
    /// Creates a new PowerServiceImpl with the default value of the power state being POWER_OFF_EXIST.
    /// The POWER_OFF_EXIT state is the only possible state at this step because calling new() means we are coming from cold boot.
    /// If we resume from S2R or S2D new() should not be called again.
    pub fn new(
        comms: Arc<dyn sdv_mw_rs_comms::Communicate>,
        vm_name: &str,
    ) -> anyhow::Result<Self> {
        Self::new_with_notifier(Arc::new(PowerStateNotifierImpl::new(comms, vm_name)))
    }

    fn new_with_notifier(
        power_state_notifier: Arc<dyn PowerStateNotifier>,
    ) -> anyhow::Result<Self> {
        //TODO: check which suspend services is provided by the Host.

        // Channel to forward power state requests.
        let (power_state_request_sender, power_state_request_receiver) =
            channel::<PowerRequest>(POWER_STATE_CHANNEL_QUEUE_SIZE);

        // Watch channel that holds the value for current power state.
        let (power_state_watch_sender, power_state_watch_receiver) =
            watch_channel::<Arc<dyn PowerState>>(get_persisted_power_state());

        power_state_notifier.create_binder_service(power_state_watch_receiver.clone())?;
        log::debug!("sdv_vpm_agent has started successfully");
        tracing::event!(Level::DEBUG, "sdv_vpm_agent has started successfully");
        Ok(Self {
            power_state_notifier,
            power_state_request_sender,
            power_state_request_receiver,
            power_state_watch_sender: power_state_watch_sender.into(),
            power_state_watch_receiver,
            power_state_tracker: PowerStateTracker::new(),
        })
    }

    // Instrument somehow messes clippy and generates this lint error.
    #[allow(clippy::async_yields_async)]
    #[instrument(skip(self))]
    // Power state request should be already validated processing the new power state.
    async fn process_power_request(
        &self,
        power_request: PowerRequest,
    ) -> JoinHandle<anyhow::Result<()>> {
        let cloned_power_state_notifier = self.power_state_notifier.clone();
        let cloned_power_state_watch_sender = self.power_state_watch_sender.clone();
        let cloned_power_state_request_sender = self.power_state_request_sender.clone();
        let cloned_cancellation_token: CancellationToken =
            self.power_state_tracker.cancel_token.clone();
        tokio::spawn(async move {
            debug!(
                "Handling new power state {:?} with timeout {:?}!",
                power_request.power_state.get_report(),
                power_request.timeout_ms
            );

            // Setting the power state so that it can be cancelled if necessary.
            let new_state: Arc<dyn PowerState> = Arc::from(power_request.power_state);
            cloned_power_state_watch_sender.send(new_state.clone())?;
            persist_power_state(new_state.as_ref()).unwrap_or_else(|e| {
                error!("Could not persist power state: {new_state:?}. error: {e}");
            });

            let select_result = tokio::select! {
                 _ = sleep(Duration::from_millis(power_request.timeout_ms)) => {
                     debug!("Timeout reached");
                     Err(PowerStateNotificationError::TimedOut)
                 },
                 _ = cloned_cancellation_token.cancelled() => {
                    debug!("Power state transition was  cancelled");
                    Err(PowerStateNotificationError::Cancelled)
                },
                // Notify SDV agent callbacks.
                result  = cloned_power_state_notifier.notify(new_state.get_report()) => {
                    result
                }
            };

            // Interrupted states which have been cancelled should not be set or notified to OEM.
            // After this point cancellation does not make sense.
            // All callbacks have been already notified.
            if cloned_cancellation_token.is_cancelled() && select_result.as_ref().is_err() {
                debug!("State was interrupted and cancelled {:?}", new_state.get_report());
                return Err(anyhow!("The state was interrupted and cancelled."));
            }

            debug!("Start post state execution of {:?}", new_state.get_report());
            let execution_result = new_state
                .execute(cloned_power_state_request_sender, power_request.timeout_ms)
                .await;

            if execution_result.is_err() {
                error!("Could not execute state.");
            }

            // We don't need to notify OEM about sleep reports, because new_state.execute will suspend the VM including this process or power-off VM.
            // If the VM suspends, the following code will only be executed once the process resumes from suspension, therefore the report will be outdated.
            // The new_state.execute will also request a transition to the new resume state which will be received by the power_state_request_receiver.
            if !new_state.is_sleep_state() {
                notify_oem(new_state, select_result, cloned_power_state_notifier).await;
            }

            Ok(())
        })
    }
}

#[instrument(skip(cloned_power_state_notifier))]
async fn notify_oem(
    new_state: Arc<dyn PowerState>,
    select_result: anyhow::Result<(), PowerStateNotificationError>,
    cloned_power_state_notifier: Arc<dyn PowerStateNotifier>,
) {
    match select_result {
        Ok(_) => {
            info!(
                "Successfully notified all callbacks, notifying OEM for {:?}!",
                new_state.get_report()
            );
            let result = cloned_power_state_notifier
                .notify_oem(new_state.get_report(), PowerStateChangeReason::HOST_REQUESTED)
                .await;
            debug!("OEM state update result {:?} {:?}", new_state.get_report(), result);
        }
        Err(PowerStateNotificationError::TimedOut) => {
            debug!("Call to notify callbacks has timed-out!");
            let result = cloned_power_state_notifier
                .notify_oem(new_state.get_report(), PowerStateChangeReason::TIMEOUT)
                .await;
            debug!("OEM state update result {:?} {:?}", new_state.get_report(), result);
        }
        Err(e) => {
            debug!("Failed to notify one of the callbacks! error={e}");
            let result = cloned_power_state_notifier
                //TODO: b/353655403 Add more reasons for failing callbacks.
                .notify_oem(new_state.get_report(), PowerStateChangeReason::TIMEOUT)
                .await;
            debug!(
                "OEM was notified about power state update {:?} {:?}",
                new_state.get_report(),
                result
            );
        }
    }
}

#[async_trait]
impl PowerService for PowerServiceImpl {
    fn get_power_state_request_sender(&self) -> Sender<PowerRequest> {
        self.power_state_request_sender.clone()
    }

    fn get_power_state_watch_receiver(&self) -> WatchReceiver<Arc<dyn PowerState>> {
        self.power_state_watch_receiver.clone()
    }

    async fn start(&mut self) -> anyhow::Result<()> {
        // The `power_state_request` channel has a size of 1 message which means that only one power request can be received and can be processed at any point in time.

        // While a power state is processing we can receive new power state requests to cancel/interrupt the previous state
        // as long as they are valid in the power state machine flow. This is guaranteed by power state machine validation. New requests will be validated against the current executed state of the watch channel.
        // For cancellation we need to stop processing of the previous state. The cancellation tokens are cancelled and the task of the already running power state is aborted.
        while let Some(power_request) = self.power_state_request_receiver.recv().await {
            let new_power_report = power_request.power_state.get_report();
            if let Some(power_state_process) = self.power_state_tracker.power_state_handle.take() {
                if power_request.power_state.get_report() == PowerStateReport::SHUTDOWN_CANCELLED {
                    // Cancel the cancellation token to stop the progress of ongoing request.
                    // No need to abort the already executing task `power_state_process` because cancel token should handle the cancellation cases.
                    self.power_state_tracker.cancel_token.cancel();

                    // The power state process could still finish successfully if its already passed cancellation points.
                    let result = power_state_process.await;
                    debug!(
                        "Aborted power state {:?} because a cancel was received {:?}",
                        self.power_state_tracker.power_report, result
                    );
                } else {
                    debug!("Waiting for power state to complete before executing the next one!");
                    let result = power_state_process.await;
                    debug!(
                        "Executed power state {:?} result {:?}",
                        self.power_state_tracker.power_report, result
                    );
                }
            }
            // We need to create a new tracker before setting the task because it needs a new cancellation token set to self before we can process the request.
            // Otherwise process_power_request will use the old token
            self.power_state_tracker = PowerStateTracker::new();
            self.power_state_tracker.set_power_state_handle(
                self.process_power_request(power_request).await,
                new_power_report,
            );
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct PowerRequest {
    pub power_state: Box<dyn PowerState>,
    pub timeout_ms: u64,
}
impl PowerRequest {
    pub fn new(power_state: Box<dyn PowerState>, timeout_ms: u64) -> Self {
        Self { power_state, timeout_ms }
    }
}

#[automock]
#[async_trait]
pub trait PowerStateRequestHandler: Send + Sync {
    /// Requests a state change in the power state machine from the VM owner.
    /// returns Ok() if the request passes the state machine validation.
    /// otherwise returns an error.
    async fn handle_set_power_state(&self, request: PowerStateChangeRequest) -> anyhow::Result<()>;

    /// Gets the current power state
    async fn get_current_power_state(&self) -> PowerStateReport;
}

pub struct PowerStateRequestHandlerImpl {
    power_state_request_sender: Sender<PowerRequest>,
    power_state_watch_receiver: WatchReceiver<Arc<dyn PowerState>>,
}

impl PowerStateRequestHandlerImpl {
    pub fn new(
        power_state_request_sender: Sender<PowerRequest>,
        power_state_watch_receiver: WatchReceiver<Arc<dyn PowerState>>,
    ) -> Self {
        Self { power_state_request_sender, power_state_watch_receiver }
    }
}

#[async_trait]
impl PowerStateRequestHandler for PowerStateRequestHandlerImpl {
    async fn handle_set_power_state(&self, request: PowerStateChangeRequest) -> anyhow::Result<()> {
        let power_request = validate_power_state_request(request)?;

        let current_state = self.power_state_watch_receiver.borrow().clone();
        if !current_state.is_valid_next_state(power_request.power_state.get_report()) {
            error!(
                "Power state transition is invalid, current={:?} requested {:?}",
                current_state.get_report(),
                power_request.power_state.get_report()
            );
            return Err(anyhow!("Power request was not specified!"));
        }

        debug!(
            "Request for power state change {:?} approved!",
            power_request.power_state.get_report()
        );

        self.power_state_request_sender.send(power_request).await?;
        Ok(())
    }

    async fn get_current_power_state(&self) -> PowerStateReport {
        self.power_state_watch_receiver.borrow().get_report()
    }
}

fn validate_power_state_request(request: PowerStateChangeRequest) -> anyhow::Result<PowerRequest> {
    let power_state: Box<dyn PowerState> = match request
        .power_request
        .enum_value()
        .map_err(|e| anyhow!("PowerStateRequest Enum invalid value: {e}"))?
    {
        PowerStateRequest::TURN_ON => Box::new(On {}),
        PowerStateRequest::CANCEL_SHUTDOWN => Box::new(ShutdownCancelled {}),
        PowerStateRequest::PREPARE_SHUTDOWN => match request
            .shutdown_type
            .enum_value()
            .map_err(|e| anyhow!("ShutdownType Enum invalid value: {e}"))?
        {
            ShutdownType::POWER_OFF => Box::new(PowerOffEnter {}),
            ShutdownType::SUSPEND_TO_RAM => Box::new(SuspendToRamEnter {}),
            ShutdownType::SUSPEND_TO_DISK => Box::new(SuspendToDiskEnter {}),
            ShutdownType::SHUTDOWN_TYPE_UNSPECIFIED => {
                return Err(anyhow!("Shutdown type was not specified!"))
            }
        },
        PowerStateRequest::FINISH_SHUTDOWN => match request
            .shutdown_type
            .enum_value()
            .map_err(|e| anyhow!("ShutdownType Enum invalid value: {e}"))?
        {
            ShutdownType::POWER_OFF => Box::new(PowerOffPostFinish {}),
            ShutdownType::SUSPEND_TO_RAM => Box::new(SuspendToRamPostFinish {}),
            ShutdownType::SUSPEND_TO_DISK => Box::new(SuspendToDiskPostFinish {}),
            ShutdownType::SHUTDOWN_TYPE_UNSPECIFIED => {
                return Err(anyhow!("Shutdown type was not specified!"))
            }
        },
        PowerStateRequest::REQUEST_UNSPECIFIED => {
            return Err(anyhow!("Power request was not specified!"))
        }
    };

    // Suspend to disk is not supported in Concorde scope.
    // TODO(b/348193447)
    if power_state.get_report() == PowerStateReport::SUSPEND_TO_DISK_ENTER
        || power_state.get_report() == PowerStateReport::SUSPEND_TO_DISK_POST_FINISH
    {
        return Err(anyhow!("Suspend to disk is not supported!"));
    }

    if request.timeout_ms == 0 {
        return Err(anyhow!("Timeout cannot be Zero!"));
    }

    Ok(PowerRequest::new(power_state, request.timeout_ms))
}

#[cfg(test)]
mod tests {
    use super::validate_power_state_request;
    use crate::power_service::PowerRequest;
    use crate::power_service::PowerService;
    use crate::power_service::PowerStateRequestHandler;
    use crate::power_state::MockPowerState;
    use crate::power_state::*;
    use crate::power_state_notifier::MockPowerStateNotifier;
    use crate::power_state_notifier::PowerStateNotificationError;
    use crate::PowerServiceImpl;
    use crate::PowerStateRequestHandlerImpl;
    use googletest::fail;
    use sdv_vpm_api::power::PowerStateReport;
    use sdv_vpm_api::vpm::PowerStateChangeRequest;
    use sdv_vpm_api::vpm::PowerStateRequest;
    use sdv_vpm_api::vpm::ShutdownType;
    use sdv_vpm_api::vpm_notifications::power_state_notification_request::PowerStateChangeReason;
    use std::sync::Arc;
    use tokio::sync::mpsc::channel;
    use tokio::sync::watch::channel as WatchChannel;
    use tokio::time::timeout;
    use tokio::time::Duration;

    use crate::power_service::notify_oem;
    use anyhow::anyhow;
    use protobuf::EnumOrUnknown;
    use sdv_comms::id::ServiceFqin;
    use sdv_comms::ContextRef;
    use sdv_mw_rs_sdv_comms::SdvComms;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn new_creates_power_service_with_initial_state() {
        // This test relies on the test environment being able to handle binder calls.

        // Given
        let fqin =
            ServiceFqin::from_str("vm:com.sdv.google.vpm.VpmPowerServiceBundle/default").unwrap();
        let context = ContextRef::create(fqin);
        let comms = Arc::new(SdvComms { context });
        let self_fqin = comms.context.get_self_fqin();
        let vm_name = self_fqin.get_sdv_vm_name();

        // Given the persistent power state is the default (PowerOffExit)
        persist_power_state(&PowerOffExit {}).expect("Failed to set default power state");

        // When
        let power_service = PowerServiceImpl::new(comms, vm_name).unwrap();

        // Then
        // The service is created, and we can access its channels.
        let _request_sender = power_service.get_power_state_request_sender();
        let watch_receiver = power_service.get_power_state_watch_receiver();

        // The initial state should be the default persisted state, which is PowerOffExit.
        assert_eq!(watch_receiver.borrow().get_report(), PowerStateReport::POWER_OFF_EXIT);
    }

    #[tokio::test]
    async fn new_with_notifier_initializes_and_creates_binder_service() {
        // Given a mock power state notifier.
        let mut mock_power_state_notifier = MockPowerStateNotifier::new();
        mock_power_state_notifier.expect_create_binder_service().times(1).returning(|_| Ok(()));

        // Given the persistent power state is the default (PowerOffExit)
        persist_power_state(&PowerOffExit {}).expect("Failed to set default power state");

        // When creating a new PowerServiceImpl with the mock notifier.
        let service =
            PowerServiceImpl::new_with_notifier(Arc::new(mock_power_state_notifier)).unwrap();

        // Then the initial power state is the default (PowerOffExit), and we can get the channels.
        assert_eq!(
            service.get_power_state_watch_receiver().borrow().get_report(),
            PowerStateReport::POWER_OFF_EXIT
        );
        let _ = service.get_power_state_request_sender();
    }

    #[tokio::test]
    async fn set_power_state_succeeds() {
        // Given: the power state request handler.
        let new_report = PowerStateReport::ON;
        let mut new_state = MockPowerState::default();
        let mut power_state_notifier = MockPowerStateNotifier::default();

        power_state_notifier.expect_create_binder_service().times(1).returning(|_| Ok(()));
        // expect callback notifications to be executed.
        power_state_notifier
            .expect_notify()
            .times(1)
            .withf(move |report| *report == new_report)
            .returning(move |_| Box::pin(async { Ok(()) }));

        // expect OEM to be notified.
        power_state_notifier
            .expect_notify_oem()
            .times(1)
            .withf(move |report, reason| {
                *report == new_report && *reason == PowerStateChangeReason::HOST_REQUESTED
            })
            .returning(move |_, _| Box::pin(async { Ok(()) }));

        // expect report to be ON.
        new_state.expect_get_report().returning(move || new_report);
        // epect sleep state to be a non-sleep state.
        new_state.expect_is_sleep_state().returning(|| false);
        // expect new state to be executed.
        new_state.expect_execute().times(1).returning(|_, _| Ok(()));
        // expect new state persistency tag to be retrieved.
        new_state.expect_get_persistence_tag().times(1).returning(|| "");

        let mut power_service =
            PowerServiceImpl::new_with_notifier(Arc::new(power_state_notifier)).unwrap();
        let power_state_request_sender = power_service.get_power_state_request_sender();
        let mut power_state_watch_receiver = power_service.get_power_state_watch_receiver();
        let _handle = tokio::spawn(async move { power_service.start().await });

        // When: a power state request is requested
        let _ = power_state_request_sender.send(PowerRequest::new(Box::new(new_state), 1)).await;

        // Then: power state is set.
        if (timeout(
            Duration::from_secs(5),
            power_state_watch_receiver.wait_for(|state| state.get_report() == new_report),
        )
        .await)
            .is_err()
        {
            fail!("Test timed out").unwrap();
        }
        assert_eq!(power_state_watch_receiver.borrow().get_report(), new_report);
    }

    #[tokio::test]
    async fn set_power_state_notify_oem_fail_with_disconnect() {
        set_power_state_callbacks_fail_with_error_send_timeout(
            PowerStateNotificationError::Disconnected,
        )
        .await
    }

    #[tokio::test]
    async fn set_power_state_notify_oem_fail_with_timeout() {
        set_power_state_callbacks_fail_with_error_send_timeout(
            PowerStateNotificationError::TimedOut,
        )
        .await
    }

    async fn set_power_state_callbacks_fail_with_error_send_timeout(
        err: PowerStateNotificationError,
    ) {
        // Given: the power state request handler.
        let new_report = PowerStateReport::ON;
        let mut new_state = MockPowerState::default();
        let mut power_state_notifier = MockPowerStateNotifier::default();

        let notify_oem_executed = Arc::new(AtomicBool::new(false));
        let notify_oem_executed_closure_clone = Arc::clone(&notify_oem_executed);

        power_state_notifier.expect_create_binder_service().times(1).returning(|_| Ok(()));
        // expect callback notifications to be executed.
        power_state_notifier
            .expect_notify()
            .times(1)
            .withf(move |report| *report == new_report)
            .returning(move |_| {
                let err = err.clone();
                Box::pin(async move { Err(err) })
            });

        // expect OEM to be notified.
        power_state_notifier
            .expect_notify_oem()
            .times(1)
            .withf(move |report, reason| {
                *report == new_report && *reason == PowerStateChangeReason::TIMEOUT
            })
            .returning(move |_, _| {
                let notify_oem_executed_closure_clone =
                    Arc::clone(&notify_oem_executed_closure_clone);
                Box::pin(async move {
                    notify_oem_executed_closure_clone.swap(true, Ordering::Relaxed);
                    Ok(())
                })
            });

        // expect report to be ON.
        new_state.expect_get_report().returning(move || new_report);
        // epect sleep state to be a non-sleep state.
        new_state.expect_is_sleep_state().returning(|| false);
        // expect new state to be executed.
        new_state.expect_execute().times(1).returning(|_, _| Ok(()));
        // expect new state persistency tag to be retrieved.
        new_state.expect_get_persistence_tag().times(1).returning(|| "");

        let mut power_service =
            PowerServiceImpl::new_with_notifier(Arc::new(power_state_notifier)).unwrap();
        let power_state_request_sender = power_service.get_power_state_request_sender();
        let _handle = tokio::spawn(async move { power_service.start().await });

        // When: a power state request is requested
        let _ = power_state_request_sender.send(PowerRequest::new(Box::new(new_state), 1)).await;

        // Wait for the flag to be set, with a timeout
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            while !notify_oem_executed.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        assert!(result.is_ok(), "Test timed out before notify_oem_executed was set!");
    }

    #[tokio::test]
    async fn set_sleep_state_no_oem_notification_sent() {
        // Given: the power state request handler.
        let new_report = PowerStateReport::SUSPEND_TO_RAM_POST_FINISH;
        let mut new_state = MockPowerState::default();
        let mut power_state_notifier = MockPowerStateNotifier::default();

        // expect callback notifications to be executed.
        power_state_notifier.expect_create_binder_service().times(1).returning(|_| Ok(()));
        power_state_notifier
            .expect_notify()
            .times(1)
            .withf(move |report| *report == new_report)
            .returning(move |_| Box::pin(async { Ok(()) }));

        // expect report to be SuspendToRam.
        new_state.expect_get_report().returning(move || new_report);
        // epect sleep state to be a sleep state.
        new_state.expect_is_sleep_state().returning(|| true);
        // expect new state to be executed.
        new_state.expect_execute().times(1).returning(|_, _| Ok(()));
        // expect new state persistency tag to be retrieved.
        new_state.expect_get_persistence_tag().times(1).returning(|| "");

        let mut power_service =
            PowerServiceImpl::new_with_notifier(Arc::new(power_state_notifier)).unwrap();
        let power_state_request_sender = power_service.get_power_state_request_sender();
        let mut power_state_watch_receiver = power_service.get_power_state_watch_receiver();
        let _handle = tokio::spawn(async move { power_service.start().await });

        // When: a power state request is requested
        let _ = power_state_request_sender.send(PowerRequest::new(Box::new(new_state), 1)).await;

        // Then: power state is set.
        if (timeout(
            Duration::from_secs(5),
            power_state_watch_receiver.wait_for(|state| state.get_report() == new_report),
        )
        .await)
            .is_err()
        {
            fail!("Test timed out").unwrap();
        }
        assert_eq!(power_state_watch_receiver.borrow().get_report(), new_report);
    }

    #[tokio::test]
    async fn handle_power_state_fails_invalid_timeout() {
        // Given: the power state request handler.
        let (tx, _) = channel::<PowerRequest>(1);
        let (_, rx) = WatchChannel::<Arc<dyn PowerState>>(Arc::new(PowerOffExit {}));
        let power_state_request_handler = PowerStateRequestHandlerImpl::new(tx, rx);

        // When: a prepare power state request without a timeout is sent.
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::PREPARE_SHUTDOWN.into(),
            shutdown_type: ShutdownType::SUSPEND_TO_RAM.into(),
            ..Default::default()
        };

        // Then: power state is set.
        let result = power_state_request_handler.handle_set_power_state(request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn handle_power_state_fails_unspecified_power_state() {
        // Given: the power state request handler.
        let (tx, _) = channel::<PowerRequest>(1);
        let (_, rx) = WatchChannel::<Arc<dyn PowerState>>(Arc::new(PowerOffExit {}));
        let power_state_request_handler = PowerStateRequestHandlerImpl::new(tx, rx);

        // When: a power state request is sent without power state.
        let request = PowerStateChangeRequest { ..Default::default() };

        // Then: power state is set.
        let result = power_state_request_handler.handle_set_power_state(request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn handle_power_state_fails_prepare_no_shutdown_type() {
        // Given: the power state request handler.
        let (tx, _) = channel::<PowerRequest>(1);
        let (_, rx) = WatchChannel::<Arc<dyn PowerState>>(Arc::new(PowerOffExit {}));
        let power_state_request_handler = PowerStateRequestHandlerImpl::new(tx, rx);

        // When: a prepare power state request is sent without shutdown type.
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::PREPARE_SHUTDOWN.into(),
            timeout_ms: 1,
            ..Default::default()
        };

        // Then: power state is set.
        let result = power_state_request_handler.handle_set_power_state(request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn handle_power_state_fails_turn_off_no_shutdown_type() {
        // Given: the power state request handler.
        let (tx, _) = channel::<PowerRequest>(1);
        let (_, rx) = WatchChannel::<Arc<dyn PowerState>>(Arc::new(PowerOffExit {}));
        let power_state_request_handler = PowerStateRequestHandlerImpl::new(tx, rx);

        // When: a power off state request is sent without shutdown type.
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::FINISH_SHUTDOWN.into(),
            timeout_ms: 1,
            ..Default::default()
        };

        // Then: power state is set.
        let result = power_state_request_handler.handle_set_power_state(request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn handle_power_state_on() {
        // Given: the power state request handler.
        let (tx, mut rx) = channel::<PowerRequest>(1);
        let (_, wrx) = WatchChannel::<Arc<dyn PowerState>>(Arc::new(PowerOffExit {}));
        let power_state_request_handler = PowerStateRequestHandlerImpl::new(tx, wrx);

        // When: a power state request is sent.
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::TURN_ON.into(),
            timeout_ms: 1,
            ..Default::default()
        };

        // Then: power state is set.
        let result = power_state_request_handler.handle_set_power_state(request).await;
        assert_eq!(
            PowerStateReport::ON,
            rx.recv().await.expect("Nothing was received!").power_state.get_report()
        );
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn handle_power_state_prepare_s2r() {
        // Given: the power state request handler.
        let (tx, mut rx) = channel::<PowerRequest>(1);
        let (_, wrx) = WatchChannel::<Arc<dyn PowerState>>(Arc::new(On {}));
        let power_state_request_handler = PowerStateRequestHandlerImpl::new(tx, wrx);

        // When: a power state request is sent.
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::PREPARE_SHUTDOWN.into(),
            shutdown_type: ShutdownType::SUSPEND_TO_RAM.into(),
            timeout_ms: 1,
            ..Default::default()
        };

        // Then: power state is set.
        let result = power_state_request_handler.handle_set_power_state(request).await;
        assert_eq!(
            PowerStateReport::SUSPEND_TO_RAM_ENTER,
            rx.recv().await.expect("Nothing was received!").power_state.get_report()
        );
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn handle_power_state_prepare_s2d() {
        // Given: the power state request handler.
        let (tx, _rx) = channel::<PowerRequest>(1);
        let (_, wrx) = WatchChannel::<Arc<dyn PowerState>>(Arc::new(On {}));
        let power_state_request_handler = PowerStateRequestHandlerImpl::new(tx, wrx);

        // When: a power state request is sent.
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::PREPARE_SHUTDOWN.into(),
            shutdown_type: ShutdownType::SUSPEND_TO_DISK.into(),
            timeout_ms: 1,
            ..Default::default()
        };

        // Then: power state is set.
        let result = power_state_request_handler.handle_set_power_state(request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn handle_power_state_prepare_off() {
        // Given: the power state request handler.
        let (tx, mut rx) = channel::<PowerRequest>(1);
        let (_, wrx) = WatchChannel::<Arc<dyn PowerState>>(Arc::new(On {}));
        let power_state_request_handler = PowerStateRequestHandlerImpl::new(tx, wrx);

        // When: a power state request is sent.
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::PREPARE_SHUTDOWN.into(),
            shutdown_type: ShutdownType::POWER_OFF.into(),
            timeout_ms: 1,
            ..Default::default()
        };

        // Then: power state is set.
        let result = power_state_request_handler.handle_set_power_state(request).await;
        assert_eq!(
            PowerStateReport::POWER_OFF_ENTER,
            rx.recv().await.expect("Nothing was received!").power_state.get_report()
        );
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn handle_power_state_cancel_shutdown() {
        // Given: the power state request handler.
        let (tx, mut rx) = channel::<PowerRequest>(1);
        let (_, wrx) = WatchChannel::<Arc<dyn PowerState>>(Arc::new(PowerOffEnter {}));
        let power_state_request_handler = PowerStateRequestHandlerImpl::new(tx, wrx);

        // When: a power state request is sent.
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::CANCEL_SHUTDOWN.into(),
            timeout_ms: 1,
            ..Default::default()
        };

        // Then: power state is set.
        let result = power_state_request_handler.handle_set_power_state(request).await;
        assert_eq!(
            PowerStateReport::SHUTDOWN_CANCELLED,
            rx.recv().await.expect("Nothing was received!").power_state.get_report()
        );
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn handle_power_state_s2r() {
        // Given: the power state request handler.
        let (tx, mut rx) = channel::<PowerRequest>(1);
        let (_, wrx) = WatchChannel::<Arc<dyn PowerState>>(Arc::new(WaitForFinishSuspendToRam {}));
        let power_state_request_handler = PowerStateRequestHandlerImpl::new(tx, wrx);

        // When: a power state request is sent.
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::FINISH_SHUTDOWN.into(),
            shutdown_type: ShutdownType::SUSPEND_TO_RAM.into(),
            timeout_ms: 1,
            ..Default::default()
        };

        // Then: power state is set.
        let result = power_state_request_handler.handle_set_power_state(request).await;
        assert_eq!(
            PowerStateReport::SUSPEND_TO_RAM_POST_FINISH,
            rx.recv().await.expect("Nothing was received!").power_state.get_report()
        );
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn handle_power_state_s2d() {
        // Given: the power state request handler.
        let (tx, _rx) = channel::<PowerRequest>(1);
        let (_, wrx) = WatchChannel::<Arc<dyn PowerState>>(Arc::new(WaitForFinishSuspendToDisk {}));
        let power_state_request_handler = PowerStateRequestHandlerImpl::new(tx, wrx);

        // When: a power state request is sent.
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::FINISH_SHUTDOWN.into(),
            shutdown_type: ShutdownType::SUSPEND_TO_DISK.into(),
            timeout_ms: 1,
            ..Default::default()
        };

        // Then: power state is set.
        let result = power_state_request_handler.handle_set_power_state(request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn handle_power_state_off() {
        // Given: the power state request handler.
        let (tx, mut rx) = channel::<PowerRequest>(1);
        let (_, wrx) = WatchChannel::<Arc<dyn PowerState>>(Arc::new(WaitForFinishPowerOff {}));
        let power_state_request_handler = PowerStateRequestHandlerImpl::new(tx, wrx);

        // When: a power state request is sent.
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::FINISH_SHUTDOWN.into(),
            shutdown_type: ShutdownType::POWER_OFF.into(),
            timeout_ms: 1,
            ..Default::default()
        };

        // Then: power state is set.
        let result = power_state_request_handler.handle_set_power_state(request).await;
        assert_eq!(
            PowerStateReport::POWER_OFF_POST_FINISH,
            rx.recv().await.expect("Nothing was received!").power_state.get_report()
        );
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn handle_get_power_state() {
        // Given: the power state request handler.
        let (tx, _) = channel::<PowerRequest>(1);
        let (_, wrx) = WatchChannel::<Arc<dyn PowerState>>(Arc::new(On {}));
        let power_state_request_handler = PowerStateRequestHandlerImpl::new(tx, wrx);

        //When we get the current power state request.
        let result = power_state_request_handler.get_current_power_state().await;

        // Then: power state is returned.
        assert_eq!(PowerStateReport::ON, result);
    }

    #[tokio::test]
    async fn process_power_request_handles_execution_error() {
        // Given a power service
        let mut mock_notifier = MockPowerStateNotifier::default();
        mock_notifier.expect_create_binder_service().returning(|_| Ok(()));
        mock_notifier.expect_notify().times(1).returning(move |_| Box::pin(async { Ok(()) }));
        mock_notifier
            .expect_notify_oem()
            .times(1)
            .returning(move |_, _| Box::pin(async { Ok(()) }));

        let service = PowerServiceImpl::new_with_notifier(Arc::new(mock_notifier)).unwrap();

        // And a power state that fails on execution
        let mut failing_state = MockPowerState::new();
        failing_state.expect_get_report().return_const(PowerStateReport::ON);
        failing_state.expect_get_persistence_tag().return_const("failing");
        failing_state.expect_is_sleep_state().return_const(false);
        failing_state.expect_execute().times(1).returning(|_, _| Err(anyhow!("execution failed")));

        let request = PowerRequest::new(Box::new(failing_state), 100);

        // When processing the request
        let handle = service.process_power_request(request).await;

        // Then the task completes successfully, as the error is handled internally.
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn notify_oem_sends_host_requested_on_success() {
        // Given a mock notifier that expects a HOST_REQUESTED reason
        let mut mock_notifier = MockPowerStateNotifier::new();
        mock_notifier
            .expect_notify_oem()
            .withf(|report, reason| {
                *report == PowerStateReport::ON && *reason == PowerStateChangeReason::HOST_REQUESTED
            })
            .times(1)
            .returning(move |_, _| Box::pin(async { Ok(()) }));

        let state = Arc::new(On {});
        let select_result = Ok(());

        // When calling notify_oem with a successful result
        notify_oem(state, select_result, Arc::new(mock_notifier)).await;

        // Then the mock expectations are met.
    }

    #[tokio::test]
    async fn notify_oem_sends_timeout_on_timeout_error() {
        // Given a mock notifier that expects a TIMEOUT reason
        let mut mock_notifier = MockPowerStateNotifier::new();
        mock_notifier
            .expect_notify_oem()
            .withf(|report, reason| {
                *report == PowerStateReport::ON && *reason == PowerStateChangeReason::TIMEOUT
            })
            .times(1)
            .returning(move |_, _| Box::pin(async { Ok(()) }));

        let state = Arc::new(On {});
        let select_result = Err(PowerStateNotificationError::TimedOut);

        // When calling notify_oem with a timeout error
        notify_oem(state, select_result, Arc::new(mock_notifier)).await;

        // Then the mock expectations are met.
    }

    #[tokio::test]
    async fn notify_oem_sends_timeout_on_other_error() {
        // Given a mock notifier that expects a TIMEOUT reason
        let mut mock_notifier = MockPowerStateNotifier::new();
        mock_notifier
            .expect_notify_oem()
            .withf(|report, reason| {
                *report == PowerStateReport::ON && *reason == PowerStateChangeReason::TIMEOUT
            })
            .times(1)
            .returning(move |_, _| Box::pin(async { Ok(()) }));

        let state = Arc::new(On {});
        let select_result = Err(PowerStateNotificationError::Disconnected);

        // When calling notify_oem with a different error
        notify_oem(state, select_result, Arc::new(mock_notifier)).await;

        // Then the mock expectations are met.
    }

    #[tokio::test]
    async fn handle_set_power_state_fails_on_invalid_transition() {
        // Given: the power state request handler with current state as ON
        let (tx, _rx) = channel::<PowerRequest>(1);
        // Current state is On
        let (_, wrx) = WatchChannel::<Arc<dyn PowerState>>(Arc::new(On {}));
        let power_state_request_handler = PowerStateRequestHandlerImpl::new(tx, wrx);

        // When: a TURN_ON request is sent, which is not a valid next state from ON
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::TURN_ON.into(),
            timeout_ms: 1,
            ..Default::default()
        };

        // Then: the request should fail
        let result = power_state_request_handler.handle_set_power_state(request).await;
        assert!(result.is_err());
        assert!(!result.err().unwrap().to_string().is_empty());
    }

    #[tokio::test]
    async fn handle_set_power_state_fails_if_channel_is_closed() {
        // Given: the power state request handler, but the receiver is dropped
        let (tx, rx) = channel::<PowerRequest>(1);
        drop(rx); // Close the channel
        let (_, wrx) = WatchChannel::<Arc<dyn PowerState>>(Arc::new(PowerOffExit {}));
        let power_state_request_handler = PowerStateRequestHandlerImpl::new(tx, wrx);

        // When: a valid power state request is sent
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::TURN_ON.into(),
            timeout_ms: 1,
            ..Default::default()
        };
        // Then: the send operation fails and returns an error
        let result = power_state_request_handler.handle_set_power_state(request).await;
        assert!(result.is_err());
    }

    async fn power_service_test_fixture_with_pending_notify_future(
        report_with_pending_notify_future: PowerStateReport,
        persistence_tag_for_state_with_pending_notify_future: &'static str,
        //TODO: b/353655403 Add more reasons for failing callbacks.
        notify_oem_reason_for_state_with_pending_notify_future: Option<PowerStateChangeReason>,
        report_to_fully_execute: PowerStateReport,
        persistence_tag_for_state_to_fully_execute: &'static str,
    ) {
        // Given
        let mut mock_notifier = MockPowerStateNotifier::default();
        mock_notifier.expect_create_binder_service().returning(|_| Ok(()));

        let notify_loop_started = Arc::new(AtomicBool::new(false));
        let notify_loop_started_closure_clone = Arc::clone(&notify_loop_started);

        // Mock calls for the first request
        mock_notifier
            .expect_notify()
            .withf(move |report| *report == report_with_pending_notify_future)
            .times(1)
            .returning(move |_| {
                notify_loop_started_closure_clone.swap(true, Ordering::Relaxed);
                Box::pin(std::future::pending())
            });
        if let Some(notify_oem_reason) = notify_oem_reason_for_state_with_pending_notify_future {
            mock_notifier
                .expect_notify_oem()
                .withf(move |report, reason| {
                    *report == report_with_pending_notify_future && *reason == notify_oem_reason
                })
                .times(1)
                .returning(move |_, _| Box::pin(async { Ok(()) }));
        }

        // Mock calls for the second request
        mock_notifier
            .expect_notify()
            .withf(move |report| *report == report_to_fully_execute)
            .times(1)
            .returning(move |_| Box::pin(async { Ok(()) }));

        mock_notifier
            .expect_notify_oem()
            .withf(move |report, reason| {
                *report == report_to_fully_execute
                    && *reason == PowerStateChangeReason::HOST_REQUESTED
            })
            .times(1)
            .returning(move |_, _| Box::pin(async { Ok(()) }));

        let mut service = PowerServiceImpl::new_with_notifier(Arc::new(mock_notifier)).unwrap();
        let sender = service.get_power_state_request_sender();
        let mut power_state_watch_receiver = service.get_power_state_watch_receiver();
        let _service_handle = tokio::spawn(async move { service.start().await });

        // When
        // Send first request
        let mut report_with_pending_notify_state = MockPowerState::new();
        report_with_pending_notify_state
            .expect_get_report()
            .return_const(report_with_pending_notify_future);
        report_with_pending_notify_state.expect_is_sleep_state().return_const(false);
        report_with_pending_notify_state.expect_execute().returning(|_, _| Ok(()));
        report_with_pending_notify_state
            .expect_get_persistence_tag()
            .return_const(persistence_tag_for_state_with_pending_notify_future);
        sender
            .send(PowerRequest::new(Box::new(report_with_pending_notify_state), 1000))
            .await
            .unwrap();

        // Wait for the non-returning notify() to be started, with a timeout
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            while !notify_loop_started.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        assert!(result.is_ok(), "Test timed out before notify_loop_started was set!");

        // Send second request
        let mut report_to_fully_execute_state = MockPowerState::new();
        report_to_fully_execute_state.expect_get_report().return_const(report_to_fully_execute);
        report_to_fully_execute_state.expect_is_sleep_state().return_const(false);
        report_to_fully_execute_state.expect_execute().returning(|_, _| Ok(()));
        report_to_fully_execute_state
            .expect_get_persistence_tag()
            .return_const(persistence_tag_for_state_to_fully_execute);
        sender
            .send(PowerRequest::new(Box::new(report_to_fully_execute_state), 1000))
            .await
            .unwrap();

        // Then: second power state is set.
        if (timeout(
            Duration::from_secs(5),
            power_state_watch_receiver
                .wait_for(|state| state.get_report() == report_to_fully_execute),
        )
        .await)
            .is_err()
        {
            fail!("Test timed out").unwrap();
        }
        assert_eq!(power_state_watch_receiver.borrow().get_report(), report_to_fully_execute);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn power_service_start_processes_multiple_requests_sequentially() {
        power_service_test_fixture_with_pending_notify_future(
            PowerStateReport::ON,
            sdv_vpm_power_persistency_tags::ON_TAG,
            Some(PowerStateChangeReason::TIMEOUT),
            PowerStateReport::POWER_OFF_ENTER,
            sdv_vpm_power_persistency_tags::POWER_OFF_ENTER_TAG,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn power_service_start_handles_cancellation() {
        power_service_test_fixture_with_pending_notify_future(
            PowerStateReport::POWER_OFF_ENTER,
            sdv_vpm_power_persistency_tags::POWER_OFF_ENTER_TAG,
            None,
            PowerStateReport::SHUTDOWN_CANCELLED,
            sdv_vpm_power_persistency_tags::SHUTDOWN_CANCELLED_TAG,
        )
        .await;
    }

    #[test]
    fn test_validate_power_state_request_turn_on() {
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::TURN_ON.into(),
            timeout_ms: 100,
            ..Default::default()
        };
        let result = validate_power_state_request(request).unwrap();
        assert_eq!(result.power_state.get_report(), PowerStateReport::ON);
        assert_eq!(result.timeout_ms, 100);
    }

    #[test]
    fn test_validate_power_state_request_cancel_shutdown() {
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::CANCEL_SHUTDOWN.into(),
            timeout_ms: 100,
            ..Default::default()
        };
        let result = validate_power_state_request(request).unwrap();
        assert_eq!(result.power_state.get_report(), PowerStateReport::SHUTDOWN_CANCELLED);
        assert_eq!(result.timeout_ms, 100);
    }

    #[test]
    fn test_validate_power_state_request_prepare_power_off() {
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::PREPARE_SHUTDOWN.into(),
            shutdown_type: ShutdownType::POWER_OFF.into(),
            timeout_ms: 100,
            ..Default::default()
        };
        let result = validate_power_state_request(request).unwrap();
        assert_eq!(result.power_state.get_report(), PowerStateReport::POWER_OFF_ENTER);
        assert_eq!(result.timeout_ms, 100);
    }

    #[test]
    fn test_validate_power_state_request_prepare_suspend_to_ram() {
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::PREPARE_SHUTDOWN.into(),
            shutdown_type: ShutdownType::SUSPEND_TO_RAM.into(),
            timeout_ms: 100,
            ..Default::default()
        };
        let result = validate_power_state_request(request).unwrap();
        assert_eq!(result.power_state.get_report(), PowerStateReport::SUSPEND_TO_RAM_ENTER);
        assert_eq!(result.timeout_ms, 100);
    }

    #[test]
    fn test_validate_power_state_request_finish_power_off() {
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::FINISH_SHUTDOWN.into(),
            shutdown_type: ShutdownType::POWER_OFF.into(),
            timeout_ms: 100,
            ..Default::default()
        };
        let result = validate_power_state_request(request).unwrap();
        assert_eq!(result.power_state.get_report(), PowerStateReport::POWER_OFF_POST_FINISH);
        assert_eq!(result.timeout_ms, 100);
    }

    #[test]
    fn test_validate_power_state_request_finish_suspend_to_ram() {
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::FINISH_SHUTDOWN.into(),
            shutdown_type: ShutdownType::SUSPEND_TO_RAM.into(),
            timeout_ms: 100,
            ..Default::default()
        };
        let result = validate_power_state_request(request).unwrap();
        assert_eq!(result.power_state.get_report(), PowerStateReport::SUSPEND_TO_RAM_POST_FINISH);
        assert_eq!(result.timeout_ms, 100);
    }

    #[test]
    fn test_validate_power_state_request_unspecified_request() {
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::REQUEST_UNSPECIFIED.into(),
            timeout_ms: 100,
            ..Default::default()
        };
        let result = validate_power_state_request(request);
        assert!(result.is_err());
        assert!(!result.err().unwrap().to_string().is_empty());
    }

    #[test]
    fn test_validate_power_state_request_prepare_unspecified_shutdown() {
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::PREPARE_SHUTDOWN.into(),
            shutdown_type: ShutdownType::SHUTDOWN_TYPE_UNSPECIFIED.into(),
            timeout_ms: 100,
            ..Default::default()
        };
        let result = validate_power_state_request(request);
        assert!(result.is_err());
        assert!(!result.err().unwrap().to_string().is_empty());
    }

    #[test]
    fn test_validate_power_state_request_finish_unspecified_shutdown() {
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::FINISH_SHUTDOWN.into(),
            shutdown_type: ShutdownType::SHUTDOWN_TYPE_UNSPECIFIED.into(),
            timeout_ms: 100,
            ..Default::default()
        };
        let result = validate_power_state_request(request);
        assert!(result.is_err());
        assert!(!result.err().unwrap().to_string().is_empty());
    }

    #[test]
    fn test_validate_power_state_request_prepare_suspend_to_disk_unsupported() {
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::PREPARE_SHUTDOWN.into(),
            shutdown_type: ShutdownType::SUSPEND_TO_DISK.into(),
            timeout_ms: 100,
            ..Default::default()
        };
        let result = validate_power_state_request(request);
        assert!(result.is_err());
        assert!(!result.err().unwrap().to_string().is_empty());
    }

    #[test]
    fn test_validate_power_state_request_finish_suspend_to_disk_unsupported() {
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::FINISH_SHUTDOWN.into(),
            shutdown_type: ShutdownType::SUSPEND_TO_DISK.into(),
            timeout_ms: 100,
            ..Default::default()
        };
        let result = validate_power_state_request(request);
        assert!(result.is_err());
        assert!(!result.err().unwrap().to_string().is_empty());
    }

    #[test]
    fn test_validate_power_state_request_zero_timeout() {
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::TURN_ON.into(),
            timeout_ms: 0,
            ..Default::default()
        };
        let result = validate_power_state_request(request);
        assert!(result.is_err());
        assert!(!result.err().unwrap().to_string().is_empty());
    }

    #[test]
    fn test_validate_power_state_request_invalid_enum_integer_for_power_request() {
        let request = PowerStateChangeRequest {
            power_request: EnumOrUnknown::<PowerStateRequest>::from_i32(-1),
            timeout_ms: 0,
            ..Default::default()
        };
        let result = validate_power_state_request(request);
        assert!(result.is_err());
        assert!(!result.err().unwrap().to_string().is_empty());
    }

    #[test]
    fn test_validate_power_state_request_invalid_enum_integer_for_shutdown_type_when_prepare_shutdown(
    ) {
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::PREPARE_SHUTDOWN.into(),
            shutdown_type: EnumOrUnknown::<ShutdownType>::from_i32(-1),
            timeout_ms: 0,
            ..Default::default()
        };
        let result = validate_power_state_request(request);
        assert!(result.is_err());
        assert!(!result.err().unwrap().to_string().is_empty());
    }

    #[test]
    fn test_validate_power_state_request_invalid_enum_integer_for_shutdown_type_when_finish_shutdown(
    ) {
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::FINISH_SHUTDOWN.into(),
            shutdown_type: EnumOrUnknown::<ShutdownType>::from_i32(-1),
            timeout_ms: 0,
            ..Default::default()
        };
        let result = validate_power_state_request(request);
        assert!(result.is_err());
        assert!(!result.err().unwrap().to_string().is_empty());
    }
}
