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

#[cfg(test)]
mod setup;
mod tests {
    use crate::setup::TestContext;
    use google_sdv_vpm::aidl::google::sdv::vpm::PowerStateReport::PowerStateReport as PowerStateReportAidl;
    use sdv_vpm_api::power::PowerStateReport;
    use sdv_vpm_api::vpm::PowerStateChangeRequest;
    use sdv_vpm_api::vpm::PowerStateRequest;
    use sdv_vpm_api::vpm::ShutdownType;
    use sdv_vpm_api::vpm_notifications::power_state_notification_request::PowerStateChangeReason;

    #[tokio::test(flavor = "multi_thread")]
    async fn cancel_power_state_prepare_request() {
        // Given A VM in Power On state.
        let mut test_context = TestContext::new("power-cancellation-test".to_string()).await;
        test_context.power_callback_preparer.setup_binder_callbacks();
        assert!(test_context.is_good_start_state());

        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::TURN_ON.into(),
            timeout_ms: 10000,
            ..Default::default()
        };

        test_context.vepsm.send_power_state_request(request).await;

        test_context.power_callback_preparer.assert_callback_received(PowerStateReportAidl::ON);
        test_context
            .vepsm
            .assert_power_notification(PowerStateReport::ON, PowerStateChangeReason::HOST_REQUESTED)
            .await;

        // When: the VM is on prepare state for ram suspension and a cancellation is requested.
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::PREPARE_SHUTDOWN.into(),
            shutdown_type: ShutdownType::SUSPEND_TO_RAM.into(),
            timeout_ms: 10000,
            ..Default::default()
        };

        test_context.vepsm.send_power_state_request(request).await;

        test_context
            .power_callback_preparer
            .assert_callback_received(PowerStateReportAidl::SUSPEND_TO_RAM_ENTER);
        test_context
            .vepsm
            .assert_power_notification(
                PowerStateReport::SUSPEND_TO_RAM_ENTER,
                PowerStateChangeReason::HOST_REQUESTED,
            )
            .await;

        test_context
            .power_callback_preparer
            .assert_callback_received(PowerStateReportAidl::WAIT_FOR_FINISH);
        test_context
            .vepsm
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

        test_context.vepsm.send_power_state_request(request).await;

        // Then: the vm is in shutdown cancelled state.
        test_context
            .vepsm
            .assert_power_notification(
                PowerStateReport::SHUTDOWN_CANCELLED,
                PowerStateChangeReason::HOST_REQUESTED,
            )
            .await;

        test_context.async_drop().await;
    }
}
