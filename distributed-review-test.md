# Distributed Review Test Fixture

This file is a disposable review fixture, not operating instructions for
NVIDIA Infra Controller. It changes no application code, configuration, or
published documentation. The test PR is closed without merging and is not
promoted upstream.

## Example Review Lifecycle

The following example describes a normal change that is approved for upstream
publication, not an action to perform for this disposable test.

1. The coding session opens a draft PR in its fork with the complete change.
2. The PR review owner publishes findings against the frozen revision.
3. The coding session resolves the findings and validates the final revision.
4. The coding session opens the upstream PR from the verified feature branch.
5. The coding session closes the fork PR without merging or deleting the branch.

The upstream PR links to the fork review discussion. The discussion and detailed
review evidence remain available until the upstream PR merges and the resource
owner verifies that cleanup is safe. Closing the fork PR does not complete the
issue or authorize deleting the feature branch.

## Failure Handling

If an upstream creation request has an uncertain result, the coding session
checks whether the PR exists before retrying. It preserves the reviewed branch
and evidence while resolving that uncertainty. It does not start another review
solely because the publication request failed.
