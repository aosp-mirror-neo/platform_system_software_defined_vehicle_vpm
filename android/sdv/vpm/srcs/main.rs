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
//! Vehicle Mode and Power Management Agent.

use google_sdv_vpm::binder;
use log::error;
use sdv_vpm::Agent;
use tokio::task;

static LOG_TAG: &str = "sdv_vpm_agent";

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    sdv_log::init_logger(LOG_TAG)?;
    sdv_tracing::init_tracing();
    binder::ProcessState::start_thread_pool();

    let res = sdv_comms_ctx::wait_for_agents();
    if let Err(ref err) = res {
        error!("wait_for_agents() failed with error: {:?}", err);
    }

    sdv_vpm::create_and_start_agent(Agent::new).await?;

    task::block_in_place(move || {
        binder::ProcessState::join_thread_pool();
    });
    Ok(())
}
