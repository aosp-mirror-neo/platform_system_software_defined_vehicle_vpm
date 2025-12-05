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
    use sdv_vpm_api::vehicle::VpmVehicleState;
    use sdv_vpm_api::vpm::PowerStateChangeRequest;
    use sdv_vpm_api::vpm::PowerStateRequest;
    use sdv_vpm_api::vpm::SetVehicleStateRequest;
    use sdv_vpm_api::vpm_notifications::power_state_notification_request::PowerStateChangeReason;

    // TODO remove multithread and use async channels for vehicle modes.
    #[tokio::test(flavor = "multi_thread")]
    async fn set_vehicle_states_and_power_states_notifies_system_clients() {
        // Given: a subscriber for vehicle state.
        let mut test_context = TestContext::new("integration-test".to_string()).await;

        // When: a new vehicle state is sent via RPC.
        let request = SetVehicleStateRequest {
            vehicle_state: VpmVehicleState::SOFTWARE_UPDATE.into(),
            ..Default::default()
        };

        // Get the current power state
        let power_state = test_context.vepsm.get_current_power_state().await;
        assert!(
            power_state == PowerStateReport::POWER_OFF_EXIT
                || power_state == PowerStateReport::SHUTDOWN_CANCELLED
        );

        test_context.vepsm.send_vehicle_state_request(request).await;

        // Then: the new vehicle state should be received by the subscriber.
        test_context.vepsm.assert_vehicle_state_received(VpmVehicleState::SOFTWARE_UPDATE);

        // Given power state SDV agents subscribers
        test_context.power_callback_preparer.setup_binder_callbacks();
        assert!(test_context.is_good_start_state());

        // When: the power state is sent via RPC from OEM.
        let request = PowerStateChangeRequest {
            power_request: PowerStateRequest::TURN_ON.into(),
            timeout_ms: 10000,
            ..Default::default()
        };
        test_context.vepsm.send_power_state_request(request).await;

        // Then: SDV agents can receive callbacks.
        test_context.power_callback_preparer.assert_callback_received(PowerStateReportAidl::ON);

        // Then: OEM receives confirmation about power state.
        test_context
            .vepsm
            .assert_power_notification(PowerStateReport::ON, PowerStateChangeReason::HOST_REQUESTED)
            .await;

        test_context.async_drop().await;
    }
}
