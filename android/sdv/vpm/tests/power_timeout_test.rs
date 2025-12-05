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
    use sdv_vpm_api::vpm_notifications::power_state_notification_request::PowerStateChangeReason;

    #[tokio::test(flavor = "multi_thread")]
    async fn set_power_state_when_callback_timeouts_switches_state_with_timeout_reason() {
        // Given a VM in a start state (Power Off Exit).
        let mut test_context = TestContext::new("power-timeout-test".to_string()).await;
        test_context
            .power_callback_preparer
            .with_sleep_on_state(PowerStateReportAidl::ON)
            .setup_binder_callbacks();
        assert!(test_context.is_good_start_state());

        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::TURN_ON.into(),
            // setting the timeout to something very low (lower than the sleep value of the callback which is 10 secs) to guarantee failure.
            timeout_ms: 1,
            ..Default::default()
        };
        // When: attempting to transition to On state
        test_context.vepsm.send_power_state_request(request).await;

        // Then: transition to On happens and timeout reason is returned.
        test_context
            .vepsm
            .assert_power_notification(PowerStateReport::ON, PowerStateChangeReason::TIMEOUT)
            .await;

        test_context.async_drop().await
    }
}
