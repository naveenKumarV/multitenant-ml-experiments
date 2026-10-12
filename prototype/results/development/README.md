# Development runs (excluded from reported totals)

- `checkpoint-smoke`: the initial torchrun standalone rendezvous did not complete; the runner was changed to use explicit IPv4 loopback and the local Gloo interface.
- `checkpoint-smoke-v2`: one-seed validation of all six recovery cases and three negative controls, before the three-seed designated run.
- `cluster-smoke`: five checks passed before the polling observer missed a short quota-release transition in the placement-timeout test. Workload API watches were added to preserve transitions.
- `cluster-smoke-v2`: nine checks passed before adding deliberate two-second worker termination flushes and explicit observations of pods remaining after quota release.
- `setup-v1.log`: fresh cluster was created, but nested-node image pulls timed out and the Volcano admission rollout exceeded its wait.
- `setup-v2.log`: image preloading exposed a Docker multi-platform index import problem. Setup was changed to export only the current runtime architecture. The completed retry is `../setup-verification.log`.

These logs are retained to make debugging and test refinement visible. They are not additional replications of the reported study.
