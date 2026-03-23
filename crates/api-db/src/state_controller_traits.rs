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
//! [`StateHistoryWriter`] appends an audit record to the entity's
//! `*_state_history` table after each state transition. It is called
//! automatically by the processor.

use std::marker::PhantomData;

use config_version::ConfigVersion;
use sqlx::PgConnection;

use crate::DatabaseResult;

/// Persists a state transition to a `*_state_history` table.
#[async_trait::async_trait]
pub trait StateHistoryWriter: Send + Sync + 'static {
    /// The entity's ID type (e.g. `SwitchId`, `RackId`).
    type Id: Send + Sync;
    /// The controller state type that gets serialized to JSONB.
    type ControllerState: Send + Sync;

    /// Append a state history record for the given entity.
    async fn persist(
        txn: &mut PgConnection,
        id: &Self::Id,
        version: ConfigVersion,
        state: &Self::ControllerState,
    ) -> DatabaseResult<()>;
}

/// A [`StateHistoryWriter`] that does nothing.
///
/// Useful in tests where no real state history table exists.
pub struct NoopStateHistoryWriter<Id, State>(PhantomData<fn(Id, State)>);

#[async_trait::async_trait]
impl<Id, State> StateHistoryWriter for NoopStateHistoryWriter<Id, State>
where
    Id: Send + Sync + 'static,
    State: Send + Sync + 'static,
{
    type Id = Id;
    type ControllerState = State;

    async fn persist(
        _txn: &mut PgConnection,
        _id: &Self::Id,
        _version: ConfigVersion,
        _state: &Self::ControllerState,
    ) -> DatabaseResult<()> {
        Ok(())
    }
}
