/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Tests for the [`StateHistoryWriter`] trait implementations.

use common::api_fixtures::create_test_env;
use config_version::ConfigVersion;
use db::state_controller_traits::StateHistoryWriter;

use crate::tests::common;

/// Verify that [`PowerShelfStateHistory::persist`] writes a row through the
/// trait interface and that the row can be read back.
#[crate::sqlx_test]
async fn test_state_history_writer_persist(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    use db::power_shelf_state_history::PowerShelfStateHistory;
    use model::power_shelf::PowerShelfControllerState;

    let env = create_test_env(pool).await;

    let mut txn = env.pool.begin().await?;
    let power_shelf_ids = db::power_shelf::list_segment_ids(&mut txn).await?;
    txn.commit().await?;

    if power_shelf_ids.is_empty() {
        return Ok(());
    }
    let id = power_shelf_ids[0];
    let version = ConfigVersion::initial();
    let state = PowerShelfControllerState::Initializing;

    let mut txn = env.pool.begin().await?;
    PowerShelfStateHistory::persist(&mut txn, &id, version, &state).await?;
    txn.commit().await?;

    let mut txn = env.pool.begin().await?;
    let histories =
        db::power_shelf_state_history::find_by_power_shelf_ids(&mut txn, &[id]).await?;
    txn.commit().await?;

    let ps_history = histories.get(&id).expect("history should exist");
    assert!(
        ps_history.iter().any(|h| h.state_version == version),
        "expected a history entry with version {version:?}"
    );

    Ok(())
}

/// Verify that multiple calls through the trait accumulate history entries.
#[crate::sqlx_test]
async fn test_state_history_writer_accumulates(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    use db::power_shelf_state_history::PowerShelfStateHistory;
    use model::power_shelf::PowerShelfControllerState;

    let env = create_test_env(pool).await;

    let mut txn = env.pool.begin().await?;
    let power_shelf_ids = db::power_shelf::list_segment_ids(&mut txn).await?;
    txn.commit().await?;

    if power_shelf_ids.is_empty() {
        return Ok(());
    }
    let id = power_shelf_ids[0];

    let states = [
        PowerShelfControllerState::Initializing,
        PowerShelfControllerState::FetchingData,
        PowerShelfControllerState::Configuring,
        PowerShelfControllerState::Ready,
    ];

    let mut version = ConfigVersion::initial();
    let mut txn = env.pool.begin().await?;
    for state in &states {
        PowerShelfStateHistory::persist(&mut txn, &id, version, state).await?;
        version = version.increment();
    }
    txn.commit().await?;

    let mut txn = env.pool.begin().await?;
    let histories =
        db::power_shelf_state_history::find_by_power_shelf_ids(&mut txn, &[id]).await?;
    txn.commit().await?;

    let ps_history = histories.get(&id).expect("history should exist");
    assert!(
        ps_history.len() >= states.len(),
        "expected at least {} history entries, got {}",
        states.len(),
        ps_history.len()
    );

    Ok(())
}
