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
//! Single source of truth of power state persystency tag definitions.
//! Used both by VPM Agent, and in integration testing
#![allow(missing_docs)]

/// android system property name
pub const PERSISTED_POWER_STATE_PROP: &str = "sdv.vpm.power.state";

/* Tags used to persist current state to disk */
pub const POWER_OFF_EXIT_TAG: &str = "POWER_OFF_EXIT";
pub const SUSPEND_TO_RAM_EXIT_TAG: &str = "SUSPEND_TO_RAM_EXIT";
pub const SUSPEND_TO_DISK_EXIT_TAG: &str = "SUSPEND_TO_DISK_EXIT";
pub const ON_TAG: &str = "ON";
pub const POWER_OFF_ENTER_TAG: &str = "POWER_OFF_ENTER";
pub const SUSPEND_TO_RAM_ENTER_TAG: &str = "SUSPEND_TO_RAM_ENTER";
pub const SUSPEND_TO_DISK_ENTER_TAG: &str = "SUSPEND_TO_DISK_ENTER";
pub const WAIT_FOR_FINISH_SUSPEND_TO_RAM_TAG: &str = "WAIT_FOR_FINISH_SUSPEND_TO_RAM";
pub const WAIT_FOR_FINISH_SUSPEND_TO_DISK_TAG: &str = "WAIT_FOR_FINISH_SUSPEND_TO_DISK";
pub const WAIT_FOR_FINISH_POWER_OFF_TAG: &str = "WAIT_FOR_FINISH_POWER_OFF";
pub const SHUTDOWN_CANCELLED_TAG: &str = "SHUTDOWN_CANCELLED";
pub const POWER_OFF_POST_FINISH_TAG: &str = "POWER_OFF_POST_FINISH";
pub const SUSPEND_TO_RAM_POST_FINISH_TAG: &str = "SUSPEND_TO_RAM_POST_FINISH";
pub const SUSPEND_TO_DISK_POST_FINISH_TAG: &str = "SUSPEND_TO_DISK_POST_FINISH";
