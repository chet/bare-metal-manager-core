# Distributed Review Lifecycle Fixture

This file is disposable input for a review-workflow test. It is not operating
guidance, product documentation, or permission to publish a change upstream.
The test pull request stays in this fork and closes without merging.

## Example for a Publishable Change

The following example describes an ordinary change with separate authorization
to open an upstream pull request after its review is complete.

1. The coding session opens a draft pull request in its fork with a signed
   snapshot and neutral requirements.
2. The review coordinator assigns a fresh PR-specific review session. That
   session publishes attributed findings, and the coding session replies with
   accepted corrections, reasons for declining feedback, or questions.
3. The coding session completes the required validation and resolves the
   findings. It pushes the final signed commit after review readers stop.
4. The coding session closes the fork pull request without merging. It keeps
   the feature branch and discussion for the upstream handoff.
5. The coding session opens the upstream pull request from that feature branch.
   If creation has an uncertain result, it checks for an existing pull request
   before retrying.
6. The PR-specific review session and its evidence remain available after
   upstream publication, so the coding session can ask follow-up questions.
7. After verified upstream merge and authorization for cleanup, the coding
   session requests cleanup through the coordinator. The reviewer cleans its
   eligible owned resources and returns a receipt. The coordinator verifies
   that receipt and the exact review session's idle state before stopping it,
   then reports the cleanup result to the coding session.

## Saved Review Evidence

For the ordinary change above, retain the review session and detailed reports
until authorized post-merge cleanup. This fork-only test has no upstream merge;
it requires separately authorized test cleanup using the same handoff. An
abandoned change likewise requires separate cleanup authorization.

In every case, normal ownership, preservation, and process checks still apply.
Closing the fork pull request alone does not authorize cleanup or delete its
discussion or reports.
