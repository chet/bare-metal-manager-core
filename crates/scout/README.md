# Scout

Scout reports local hardware and executes tasks requested by Core. The Cargo
binary name is `forge-scout`.

## Supervised NIC Firmware Updates

Core-driven profile firmware updates are submitted to the matching `mlx-firmware-supervisor` executable packaged beside `forge-scout`. This Linux helper runs a per-PCI-slot daemon with a durable local receipt ledger. The same executable also supplies bounded per-command subprocess supervision; there is no second helper binary. Read-only preflight, ordinary `mlxconfig` operations, lockdown, and legacy firmware update paths do not become durable operations through this integration.

For DPA provisioning, Core supplies a stable operation ID for its firmware state entry. Scout rejects missing or empty IDs before launching a helper, reads that ID first, and submits only after `NotFound`. Existing receipts take precedence over changed or missing live profiles. A new ID with no profile records a no-work `Complete` receipt with `flashed=false`, subject to the same admission checks. Scout reports the operation ID, state, phase, partial report, and diagnostic to Core; connection failure is not successful firmware completion or permission to submit a replacement operation.

Generic NIC work uses `MlxDeviceObserveFirmwareUpdate` and
`MlxDeviceUpdateFirmware` through the existing action dispatcher. Observation
checks the full frozen request without submitting work or probing the card.
Before first submission, Core saves the returned supervisor instance token.
A guarded update requires that same instance if the receipt is absent; a
replacement helper cannot authorize another burn. After Core stores a receipt,
subsequent polls only observe it. Reusing an ID with a different request is a
conflict for both observation and submission.

`MlxDeviceUpdateFirmware` requires a full lowercase PCI address, operation ID,
phase ID, profile, image SHA-256, and a configuration SHA-256 exactly when a
configuration source is present. Digests contain 64 lowercase hexadecimal
characters. The phase ID identifies the whole workflow, not an independently
dispatched burn or reset. Identical submissions return their receipt; changed
profiles, phase IDs, digests, or physical selectors conflict. Core supplies an
`expected_device` selector containing the frozen base MAC or GUID. The supervisor
checks it against the live card before mutation, while holding the host claim.
Different slots have independent
journals, so preserve both the original target and operation ID during recovery.
Checked operation IDs cannot use the `dpa-firmware:` prefix reserved for DPA state
entries. Missing profiles are rejected, not treated as no work. The supervisor
stages both inputs, checks their hashes, and runs read-only preflight before any
configuration or burn. Sources are unauthenticated HTTP(S), without user
information, queries, fragments, or redirects.

Reset and verification follow the supplied profile. A flash-only `Complete` receipt does not certify firmware activation or host readiness.

Scout advertises `NIC_FIRMWARE_UPDATE_V1` and `NIC_FIRMWARE_UPDATE_V2` on its
control poll. Core requires V2 for observation and guarded submission, plus V1
for the generic update command. Generic results contain a supervisor receipt,
missing-receipt observation, or bounded request error. Core accepts them only for
the authenticated host's matching operation ID, phase ID, and PCI address.
Generic results cannot update legacy DPA state or include legacy lock, profile,
or firmware fields. Legacy JSON and DPA command conversion cannot execute these
generic actions.

Scout uses `ObserverExitBehavior::Continue`: restarting Scout or losing an RPC does not cancel accepted work. Startup inventory, the automatic direct-command loop, Discovery/Reset IB cleanup, and hardware-touching operator streams acquire the same host claim as supervised firmware work. They retain it across discovery and command execution; settled errors release it, while unknown process completion retains it for operator reconciliation. Startup inventory remains pending until Core accepts it; a failed query or publication also leaves it pending. Receipt polling continues without device discovery while a claim is held. Supervisor `RESOURCE_EXHAUSTED` responses defer firmware submission or polling to a later iteration without reporting a firmware failure. Permanent receipt-capacity exhaustion instead returns `FAILED_PRECONDITION`; a generic task receives a correlated `UNAVAILABLE` request error, not an execution receipt. Standalone Scout MLX commands and preflight, legacy script updates, direct library calls, and external tools do not participate in this exclusion and must not run concurrently with supervised updates. See the [direct-claim recovery limits](../libmlx/README.md#supervised-firmware-operations) before handling a retained marker.

Startup inventory publishes full lowercase PCI addresses. If `mlxfwmanager` returns an MST alias, Scout runs `mst status` from `PATH` to resolve its exact `/dev/mst/` entry; a failed command or missing, duplicate, or invalid mapping rejects the report instead of guessing. Each discovery command has a 30-second deadline under the [firmware execution limits](../libmlx/README.md#firmwareflasher). Firmware submission and receipt polling accept only PCI addresses and never resolve aliases.

For an existing SuperNIC interface on the same host with the same MAC, Core can replace a saved MST locator with the reported full PCI address. The interface's ID, configuration, and controller state remain unchanged. An existing PCI locator is not changed; this does not implement card relocation.

Core sends a distinct `MlxDeviceApplyFirmwareOperation` command. Older Scouts ignore this command rather than treating it as an uncorrelated flash, and the legacy JSON command list omits it. New Scouts reject the old uncorrelated firmware command. Core and Scout must both support the operation command before firmware provisioning can advance; mixed versions do not provide a legacy flashing fallback.

Checked observation, stable physical selectors, and guarded submission require
supervisor protocol 4. DPA polling can still read existing receipts from
protocol-1 and protocol-2 daemons, but new DPA submissions require protocol 3 or
4. Unguarded generic commands without a physical selector retain protocol-3
submission behavior, which does not protect against replay after journal loss.
Post-reset verification
retries require Scout and helper support for the timing fields; Scouts predating
that support omit them. Restarting Scout does not replace a daemon in its
independent systemd scope. Refer to the
[protocol compatibility and replacement guidance](../libmlx/README.md#supervised-firmware-operations)
before replacing it; use updated clients to access protocol-4 daemons.

The packaged [Scout service](misc/forge-scout.service) selects `systemd-scope` to place the daemon outside Scout's service cgroup; values in its optional `/opt/forge/forge-scout.env` override that service setting. Scout PXE images regenerate this file from `/opt/forge/forge-scout.env.template` before each service start. Put persistent overrides in the template, for example `MLX_FIRMWARE_SUPERVISOR_LAUNCH=standalone`; editing the generated `.env` file does not survive the next start. Running Scout directly defaults to `standalone`, without a systemd requirement. Placement does not change for an already running daemon, and launch failure never falls back to another mode.

This supports process recovery on the existing state filesystem. The default `/run` state root and a PXE/tmpfs root can disappear on reboot. Journals retained on persistent storage remain readable after reboot but reject further mutation; they do not resume interrupted hardware work. Do not clear a journal or host claim to force another update. Refer to [supervised firmware operations](../libmlx/README.md#supervised-firmware-operations) for receipt states, exact configuration and storage bounds, cancellation semantics, and recovery limitations. The [helper prerequisites](../libmlx/README.md#firmwareflasher) cover kernel and packaging requirements.

### Core Reconciliation

The machine controller reconciles already admitted work in `NicFirmwareUpdating`.
Production admission remains disabled pending
[baseline firmware admission](https://github.com/dsx-ai-factory/infra-controller/issues/6680).
Deploy Core readers that understand this state before enabling writers; Scout
capabilities and helper protocol checks do not protect older Core state readers.
The persisted operation embeds typed profile, receipt, and observation data.
Older readers can reject added fields in those embedded types, so retaining the
operation is not a promise that a downgraded Core can reconcile it.

An admitted operation follows this reporting and reconciliation flow:

1. Core freezes the cards, exact firmware recipes, execution options, original
   access policies, and lifecycle continuation. Later profile edits do not change
   that work.
2. Core observes the supervisor before authorizing submission, then unlocks and
   processes cards sequentially. Committed controller state selects the next
   `ForgeAgentControl` action. Scout returns authenticated reports, and Core
   stores them in the shared database before authorizing later work. This flow
   does not depend on a process-local ScoutStream connection.
3. Activation follows the frozen policy. The supervisor performs a selected card
   reset; Core does not add another reset. For host reboot activation, Core first
   retains every completed receipt, then waits for Scout's `RebootCompleted`
   report. Each target requires either a supervisor reset or the shared host
   reboot, not both. Lost reboot responses can trigger a later bounded retry.
4. Core requests unfiltered inventory with the `MlxDeviceInfoReport` action and
   checks the stored report for every frozen card's identity and exact expected
   version. Ordinary inventory does not need firmware operation IDs. Core uses
   database receipt time to require a report received after the activation
   boundary. Missing reports, temporary request failures, or version mismatches
   keep the operation waiting for another pass.
5. Core restores and independently reads back the original access policy, saves
   successful baseline evidence when applicable, and resumes the recorded
   lifecycle. Conditional database writes prevent a stale iteration from
   advancing newer work.

Individual requests have deadlines; the convergence loop has no overall timeout
that turns routine reporting delays into manual recovery. A failed required
verification step still fails the operation even if inventory later matches.
Known failure restores access only when execution evidence proves that another
access command is safe. The controller reaches `Failed` only after independently
observing restored access; uncertain effects remain in the execution phase with
a manual-intervention diagnostic. A missing receipt
after helper replacement leaves a visible diagnostic and does not renew the
saved submission token. Inspect the original receipt and retained state using
the [supervisor recovery guidance](../libmlx/README.md#supervised-firmware-operations);
do not delete state or submit a new ID to bypass uncertainty.

If the original allocation is deleted, Core stops authorizing new card work,
reconciles existing effects, restores access when safe, and resumes the normal
allocation-release flow. A different allocation or changed live configuration
cannot inherit the old operation; automatic admission of that new work belongs
to [baseline firmware admission](https://github.com/dsx-ai-factory/infra-controller/issues/6680).

Access reconciliation uses `MlxDeviceReconcileFirmwareAccess` to read status,
change it when needed, and read it again. A mutation acknowledgment is not a
status observation. Reports carry the controller revision, target PCI address,
and physical selector; stale reports cannot advance a later revision. These
checks do not revoke a command already returned to another Scout process.
Live discovery must expose the expected base MAC or GUID even while locked.
If it does not, Scout reports an error without changing access. Support for
that hardware cohort requires qualification; cached identity is not substituted.

Scout gives identity discovery and the subsequent `flint` access command separate
30-second deadlines, including helper startup and output collection. Each attempt
caps each output stream at 1 MiB. Failure allows up to 5 seconds for cleanup and
5 more seconds for result delivery. An uncertain cleanup retains hardware
exclusion. These bounds apply to automatic access reconciliation, not the legacy
DPA or operator command paths. The controller can retry settled failures on later
iterations without an overall reconciliation deadline.

Automatic access reconciliation requires `mlxfwmanager` and `flint` on Scout's
inherited `PATH`. It does not use the legacy `flint` executable search or version
probes. Whole-host inventory also requires `mst` when MFT reports MST aliases.

Forced machine deletion can replace the active reconciliation state. Direct
power APIs, including tenant-accessible `InvokeInstancePower`, can interrupt
hardware work. These existing paths are not gated by the NIC firmware workflow;
recovery after using them is not guaranteed. The queued managed-host reset path
waits for safely restored `Failed` work before proceeding.

Reboot evidence follows Scout's existing boot-local reporting convention, not a
hardware boot identifier. MLX inventory reports the installed version returned by
`mlxfwmanager`; it does not independently distinguish running and pending images
or certify network readiness. Database receipt time orders report acceptance,
not collection: a delayed report can arrive after activation. Activation and
reboot-request boundaries use Core timestamps, so freshness checks require
synchronized clocks on Core replicas and the database. They do not compare
Scout's clock with those boundaries. These hardware
and reporting guarantees require
[cohort qualification](https://github.com/dsx-ai-factory/infra-controller/issues/6689).

## Read-Only NIC Firmware Preflight

The local `mlx firmware preflight` command verifies downloaded files and compares
one live NIC with an exact firmware target. It requires Linux, standalone mode,
and the matching `mlx-firmware-supervisor` executable beside `forge-scout`.
Scout packages include the helper. The command does not register with Core.
Display the complete command reference with:

```bash
forge-scout --mode standalone mlx firmware preflight --help
```

Supply the NIC's PCI address, expected part number and PSID, target firmware
version, image URL, and image SHA-256 digest. If you supply a device-config URL,
its SHA-256 digest is also required. Omitting both device-config arguments skips
that file. Digests accept exactly 64 hexadecimal digits, in either case.

Artifact sources must use HTTP or HTTPS without user information, a query, or a
fragment. Redirects are rejected. This command does not use Core credentials or
a proxy. Each present input is staged separately and checked before the device
query; equal basenames cannot overwrite each other. Temporary files are removed
when the command finishes or is cancelled by SIGINT or SIGTERM. Cleanup is not
guaranteed after SIGKILL or power loss.

Each artifact has a 600-second deadline for downloading and retries. Hashing follows that deadline. The subsequent `mlxfwmanager` query has a 30-second deadline, including helper startup, descendants, and output collection. Each output stream is capped at 1 MiB. Failure or timeout allows up to 5 seconds for helper cleanup and up to 5 more seconds to receive its result. Cancellation requests cleanup without waiting for confirmation. The helper can continue after the caller exits, but terminating the whole service control group can kill the helper too and prevent cleanup. Refer to the [firmware execution limits](../libmlx/README.md#firmwareflasher) for the shared supervisor contract.

The query must return the requested PCI address, matching part number and PSID,
and a known installed firmware version. MST aliases are not resolved.

An exact version match and a different known version both return a successful
command status, with the comparison printed in the output. A newer installed
version still differs from an older exact target. Missing facts, identity
mismatches, invalid inputs, failed downloads, or checksum mismatches return an
error.

Preflight does not apply configuration, lock or unlock a device, burn firmware,
or reset hardware. It does not interpret image or device-config contents, verify
that configuration matches the running configuration, or establish that flashing
is safe. An exact firmware match is not approval to skip a future configuration
change.
