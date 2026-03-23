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

use carbide_uuid::infiniband::IBPartitionId;
use config_version::ConfigVersion;
use model::ib_partition::IBPartitionControllerState;
use sqlx::PgConnection;

use crate::state_controller_traits::StateHistoryWriter;
use crate::{DatabaseError, DatabaseResult};

pub struct IBPartitionStateHistory;

#[async_trait::async_trait]
impl StateHistoryWriter for IBPartitionStateHistory {
    type Id = IBPartitionId;
    type ControllerState = IBPartitionControllerState;

    async fn persist(
        txn: &mut PgConnection,
        id: &IBPartitionId,
        version: ConfigVersion,
        state: &IBPartitionControllerState,
    ) -> DatabaseResult<()> {
        let next_version = version.increment();
        persist(txn, id, state, next_version).await
    }
}

pub async fn persist(
    txn: &mut PgConnection,
    partition_id: &IBPartitionId,
    state: &IBPartitionControllerState,
    state_version: ConfigVersion,
) -> DatabaseResult<()> {
    let query = "INSERT INTO ib_partition_state_history (partition_id, state, state_version) VALUES ($1, $2, $3)";
    sqlx::query(query)
        .bind(partition_id)
        .bind(sqlx::types::Json(state))
        .bind(state_version)
        .execute(txn)
        .await
        .map_err(|e| DatabaseError::query(query, e))?;
    Ok(())
}
