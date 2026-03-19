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
//! The state controller processor uses three traits to persist state transitions:
//!
//! - [`ControllerStateWriter`] — updates the entity's controller state in the
//!   main table (e.g. `switches`, `racks`, `machines`)
//! - [`StateHistoryWriter`] — appends an audit record to the entity's
//!   `*_state_history` table
//! - [`OutcomeWriter`] — stores the result of the most recent state handler
//!   iteration (wait, error, transition, etc.)
//!
//! All are called automatically by the processor.

use std::marker::PhantomData;

use config_version::ConfigVersion;
use model::controller_outcome::PersistentStateHandlerOutcome;
use sqlx::PgConnection;

use crate::DatabaseResult;

/// Updates the entity's controller state in the main table.
///
/// The processor passes both the expected (current) version for optimistic
/// locking and the new (incremented) version to store. Implementations that
/// don't use optimistic locking (e.g. machines) may ignore `expected_version`.
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
    ) -> DatabaseResult<()>;
}

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

/// Stores the outcome of the most recent state handler iteration.
#[async_trait::async_trait]
pub trait OutcomeWriter: Send + Sync + 'static {
    /// The entity's ID type.
    type Id: Send + Sync;

    /// Persist the handler outcome for the given entity.
    async fn persist(
        txn: &mut PgConnection,
        id: &Self::Id,
        outcome: PersistentStateHandlerOutcome,
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
