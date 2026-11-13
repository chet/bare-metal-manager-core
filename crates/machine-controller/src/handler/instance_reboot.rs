// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Ordinary instance reboot requests, executed by the existing machine controller.

use bmc_backend::BackendBmc;
use bmc_platform::ResetType;
use carbide_utils::redfish::BmcAccessInfo;
use futures_util::FutureExt;
use model::hardware_dispatch::{HardwareDispatch, HardwareDispatchOwner};
use model::machine::{
    MachineLastRebootRequested, MachineLastRebootRequestedMode, ManagedHostState,
    ManagedHostStateSnapshot,
};
use state_controller::db_write_batch::WriteOpFn;
use state_controller::state_handler::{
    StateHandlerContext, StateHandlerError, StateHandlerOutcome,
};

use crate::context::MachineStateHandlerContextObjects;

/// Cancel an unsent reboot when its allocation has gone away, without BMC I/O.
pub(super) async fn cancel_unsent_request(
    snapshot: &ManagedHostStateSnapshot,
    ctx: &mut StateHandlerContext<'_, MachineStateHandlerContextObjects>,
) -> Result<Option<StateHandlerOutcome<ManagedHostState>>, StateHandlerError> {
    let Some(dispatch) = pending_request(snapshot) else {
        return Ok(None);
    };
    let HardwareDispatchOwner::InstanceReboot {
        instance_id,
        last_attempt_at,
        ..
    } = &dispatch.owner
    else {
        return Ok(None);
    };
    if snapshot
        .instance
        .as_ref()
        .is_none_or(|instance| instance.id != *instance_id || instance.deleted.is_some())
    {
        if last_attempt_at.is_some() {
            // Release must still reach its existing cleanup states. Keep an uncertain
            // reboot recorded so it cannot silently allow a later NIC update.
            // Guarded recovery: https://github.com/dsx-ai-factory/infra-controller/issues/8942
            return Ok(None);
        }
        // A vanished allocation cannot leave a never-sent request excluding future NIC work.
        // Recheck under the normal lock order so a stale snapshot cannot cancel current work.
        let mut txn = ctx.services.db_pool.begin().await?;
        let instance = db::instance::find_by_id_for_update(txn.as_mut(), *instance_id).await?;
        db::machine::lock_by_id_for_update(txn.as_mut(), &snapshot.host_snapshot.id.into()).await?;
        if instance.as_ref().is_none_or(|instance| {
            instance.machine_id != snapshot.host_snapshot.id || instance.deleted.is_some()
        }) {
            settle(&mut txn, snapshot.host_snapshot.id, dispatch).await?;
            return Ok(Some(StateHandlerOutcome::do_nothing().with_txn(txn)));
        }
        return Ok(Some(
            StateHandlerOutcome::wait("instance reboot snapshot needs refreshing".into())
                .with_txn(txn),
        ));
    }
    Ok(None)
}

/// Find the queued reboot whose result still owns the host.
pub(super) fn pending_request(snapshot: &ManagedHostStateSnapshot) -> Option<&HardwareDispatch> {
    snapshot
        .host_snapshot
        .status
        .hardware_dispatch
        .as_ref()
        .filter(|dispatch| {
            dispatch.is_pending()
                && matches!(dispatch.owner, HardwareDispatchOwner::InstanceReboot { .. })
        })
}

/// Reconcile a queued reboot after the controller finishes other Assigned/Ready work.
pub(super) async fn handle(
    snapshot: &ManagedHostStateSnapshot,
    ctx: &mut StateHandlerContext<'_, MachineStateHandlerContextObjects>,
) -> Result<Option<StateHandlerOutcome<ManagedHostState>>, StateHandlerError> {
    let Some(dispatch) = pending_request(snapshot) else {
        return Ok(None);
    };
    let HardwareDispatchOwner::InstanceReboot {
        last_attempt_at, ..
    } = &dispatch.owner
    else {
        return Ok(None);
    };
    // Use the existing reboot retry interval with its one-minute minimum.
    // The interval permits another attempt; it never establishes completion.
    let retry_after = ctx
        .services
        .site_config
        .machine_state_controller
        .failure_retry_time
        .max(chrono::Duration::minutes(1));
    if let Some(attempt) = last_attempt_at {
        let now = db::machine::database_now(&ctx.services.db_pool).await?;
        if now < *attempt + retry_after {
            return Ok(Some(StateHandlerOutcome::wait(
                "waiting before reconciling the instance reboot request".into(),
            )));
        }
    }
    // The processor's max_object_handling_time bounds the whole iteration, including
    // credentials and network I/O, while its queue reservation excludes another worker.
    attempt(snapshot, dispatch, ctx).await.map(Some)
}

/// Prepare the configured BMC client, recheck the saved request, then observe or send it.
async fn attempt(
    snapshot: &ManagedHostStateSnapshot,
    dispatch: &HardwareDispatch,
    ctx: &mut StateHandlerContext<'_, MachineStateHandlerContextObjects>,
) -> Result<StateHandlerOutcome<ManagedHostState>, StateHandlerError> {
    let HardwareDispatchOwner::InstanceReboot {
        machine_id,
        instance_id,
        bmc,
        last_attempt_at,
    } = &dispatch.owner
    else {
        return Err(StateHandlerError::GenericError(eyre::eyre!(
            "expected an instance reboot request"
        )));
    };
    if *machine_id != snapshot.host_snapshot.id {
        return Err(StateHandlerError::ManualInterventionRequired(
            "instance reboot request belongs to another host".into(),
        ));
    }
    // Prepare the captured IP, port and credential identity. A newer inventory
    // lookup must not select a different client that later passes the locked recheck.
    let access = BmcAccessInfo {
        host: bmc
            .ip
            .ok_or_else(|| StateHandlerError::MissingData {
                object_id: machine_id.to_string(),
                missing: "saved BMC IP",
            })?
            .to_string(),
        port: bmc.port,
        mac_address: bmc.mac.ok_or_else(|| StateHandlerError::MissingData {
            object_id: machine_id.to_string(),
            missing: "saved BMC MAC",
        })?,
    };
    let prepared = ctx
        .services
        .connect_bmc_for_mutation((*machine_id).into(), &access)
        .await?;

    // Use the controller's existing best-effort restart observation after a lost reply.
    // A tenant OS need not run Scout. Unavailable logs do not masquerade as completion;
    // they leave the request owned while the controller makes a later reboot attempt.
    let observed_restart = if let Some(attempt) = last_attempt_at {
        match &prepared {
            BackendBmc::Libredfish(client) => {
                match super::check_restart_in_logs(client.as_ref(), *attempt).await {
                    Ok(found) => found,
                    Err(error) => {
                        tracing::debug!(host_machine_id = %machine_id, %error, "Instance reboot logs unavailable; retaining controller retry policy");
                        false
                    }
                }
            }
            BackendBmc::Drivers(_) => false,
        }
    } else {
        false
    };

    // Preparation can wait on credentials/network. Recheck Instance then Machine under
    // the same locks used by the request API and NIC ownership; release them before I/O.
    let mut txn = ctx.services.db_pool.begin().await?;
    let instance = db::instance::find_by_id_for_update(txn.as_mut(), *instance_id).await?;
    db::machine::lock_by_id_for_update(txn.as_mut(), &(*machine_id).into()).await?;
    let current = db::machine::find_one(txn.as_mut(), machine_id, Default::default())
        .await?
        .ok_or_else(|| StateHandlerError::MissingData {
            object_id: machine_id.to_string(),
            missing: "instance reboot host",
        })?;
    if current.status.hardware_dispatch.as_ref() != Some(dispatch)
        || current.state.version != snapshot.host_snapshot.state.version
    {
        return Ok(StateHandlerOutcome::wait(
            "instance reboot snapshot changed during preparation".into(),
        )
        .with_txn(txn));
    }
    let current_bmc = &current.status.bmc_info;
    let target_matches = instance
        .as_ref()
        .is_some_and(|instance| instance.machine_id == *machine_id && instance.deleted.is_none())
        && bmc.ip == current_bmc.ip
        && bmc.port == current_bmc.port
        && bmc.mac == current_bmc.mac;
    if !target_matches {
        if last_attempt_at.is_none() {
            // No sender has been authorized: cancel this stale request rather than redirect it.
            settle(&mut txn, *machine_id, dispatch).await?;
            tracing::warn!(host_machine_id = %machine_id, %instance_id, "Canceled unsent instance reboot because its target changed");
            return Ok(StateHandlerOutcome::do_nothing().with_txn(txn));
        }
        return Err(StateHandlerError::ManualInterventionRequired(format!(
            "instance reboot {} has an uncertain result and its target changed; reconcile the original host before clearing hardware ownership",
            dispatch.token,
        )));
    }
    if observed_restart {
        settle(&mut txn, *machine_id, dispatch).await?;
        let observed_at = last_attempt_at.unwrap_or(dispatch.started_at);
        // Another controller flow can have rebooted before this request is
        // reconciled. Keep that newer bookkeeping while settling the old owner.
        if current
            .status
            .last_reboot_requested
            .is_none_or(|request| request.time <= observed_at)
        {
            db::machine::record_reboot_request(
                &(*machine_id).into(),
                &mut txn,
                &MachineLastRebootRequested {
                    time: observed_at,
                    mode: MachineLastRebootRequestedMode::Reboot,
                    restart_verified: Some(true),
                    verification_attempts: Some(0),
                },
            )
            .await?;
        }
        return Ok(StateHandlerOutcome::do_nothing().with_txn(txn));
    }
    let next =
        db::hardware_dispatch::record_instance_reboot_attempt(&mut txn, *machine_id, dispatch)
            .await?
            .ok_or_else(|| {
                StateHandlerError::GenericError(eyre::eyre!("instance reboot ownership changed"))
            })?;
    txn.commit().await?;
    let HardwareDispatchOwner::InstanceReboot {
        last_attempt_at: Some(requested_at),
        ..
    } = next.owner
    else {
        return Err(StateHandlerError::GenericError(eyre::eyre!(
            "expected an instance reboot attempt"
        )));
    };

    // Any error or cancellation leaves this attempt durable. The next controller iteration
    // observes/retries the same request, rather than releasing the host to NIC maintenance.
    let sent = super::handler_host_power_control_with_bmc(
        snapshot,
        ctx,
        prepared,
        ResetType::ForceRestart,
        std::panic::Location::caller(),
        Some(requested_at),
        true,
    )
    .await?;
    if !sent {
        return Ok(StateHandlerOutcome::wait(
            "instance reboot was not sent because the host powered off".into(),
        ));
    }
    let mut txn = ctx.services.db_pool.begin().await?;
    settle(&mut txn, *machine_id, &next).await?;
    // Ordinary reboots finish on acceptance. Do not let the generic verifier send
    // another reboot when a tenant OS supplies no restart observation. Run after
    // the shared helper's tracking write, and leave any different request untouched.
    let machine_id = (*machine_id).into();
    let accepted = MachineLastRebootRequested {
        time: requested_at,
        mode: MachineLastRebootRequestedMode::Reboot,
        restart_verified: Some(false),
        verification_attempts: Some(0),
    };
    let write: WriteOpFn = Box::new(move |txn| {
        async move {
            let _ = db::machine::update_restart_verification_status(
                &machine_id,
                accepted,
                None,
                0,
                txn,
            )
            .await?;
            Ok(())
        }
        .boxed()
    });
    ctx.pending_db_writes.push(write);
    Ok(StateHandlerOutcome::do_nothing().with_txn(txn))
}

/// Clear exclusion only for the request whose response or cancellation was established.
async fn settle(
    txn: &mut sqlx::PgConnection,
    machine_id: carbide_uuid::machine::HostMachineId,
    dispatch: &HardwareDispatch,
) -> Result<(), StateHandlerError> {
    if !db::hardware_dispatch::settle(txn, machine_id, dispatch.token, &dispatch.owner).await? {
        return Err(StateHandlerError::GenericError(eyre::eyre!(
            "instance reboot ownership changed before settlement"
        )));
    }
    Ok(())
}
