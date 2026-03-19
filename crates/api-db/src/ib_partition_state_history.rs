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

use crate::DatabaseResult;
use crate::state_controller_traits::StateHistoryWriter;

/// [`StateHistoryWriter`] for IB partitions.
///
/// No state history table exists for IB partitions yet. When one is added,
/// replace this no-op with a real implementation.
pub struct IBPartitionStateHistory;

#[async_trait::async_trait]
impl StateHistoryWriter for IBPartitionStateHistory {
    type Id = IBPartitionId;
    type ControllerState = IBPartitionControllerState;

    async fn persist(
        _txn: &mut PgConnection,
        _id: &IBPartitionId,
        _version: ConfigVersion,
        _state: &IBPartitionControllerState,
    ) -> DatabaseResult<()> {
        Ok(())
    }
}
