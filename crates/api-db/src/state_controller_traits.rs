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

//! Traits for the state controller persistence framework.
//!
//! The state controller processor uses [`ControllerStateWriter`] to persist
//! controller state transitions. It is called automatically by the processor.

use config_version::ConfigVersion;
use sqlx::PgConnection;

use crate::DatabaseResult;

/// Updates the entity's controller state in the main table.
///
/// Returns `true` if the write landed, `false` if the optimistic lock didn't
/// match (i.e. another processor already transitioned the entity).
///
/// The processor passes both the expected (current) version for optimistic
/// locking and the new (incremented) version to store. Implementations that
/// don't use optimistic locking (e.g. machines) may ignore `expected_version`
/// and always return `true`.
#[async_trait::async_trait]
pub trait ControllerStateWriter: Send + Sync + 'static {
    /// The entity's ID type (e.g. `SwitchId`, `RackId`).
    type Id: Send + Sync;
    /// The controller state type that gets serialized to JSONB.
    type ControllerState: Send + Sync;

    /// Persist the new controller state for the given entity.
    async fn persist(
        txn: &mut PgConnection,
        id: &Self::Id,
        expected_version: ConfigVersion,
        new_version: ConfigVersion,
        new_state: &Self::ControllerState,
    ) -> DatabaseResult<bool>;
}
