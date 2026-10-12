# Narrow Kueue Volcano and checkpoint prototype

This artifact implements and tests a small admission bridge, quota borrowing/reclaim policies, and two-process CPU checkpoint recovery for the Informatica major revision. Read `REPORT.md` for observed results and limitations, `TEST_PLAN.md` for the test definitions, and `results/` for raw evidence.

The bridge is Python, supports fixed-size `batch/v1 Job` only, and runs as one host process against an isolated Kubernetes cluster. It persists a random attempt token in AdmissionCheck status while Pending, creates/reuses a PodGroup, and uses a resourceVersion-conditional status write to report Ready with pod-set annotations. Ready means the PodGroup contract is prepared; it does not wait for pods to exist or promise a physical reservation. Kueue supplies flavor selectors and owns Job suspension; Volcano handles gang placement. A placement timeout requests Retry. No Volcano preempt/reclaim/backfill action is enabled.

## Reproduce

Prerequisites: Python 3.12, uv (or pip), Docker 28 or newer, kind, and curl. On macOS the tested runtime is Colima. Install the development tools if absent:

```sh
brew install colima docker kind
```

From this directory:

```sh
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r requirements.lock
bash scripts/setup_cluster.sh
.venv/bin/python scripts/run_cluster.py --output results/cluster-new-run
.venv/bin/python scripts/run_checkpoint.py --output results/checkpoint-new-run --seeds 7,19,41
```

Use a new output directory for each run. The runners refuse to overwrite an existing run. Dependencies are pinned in `requirements.lock`; upstream Kubernetes manifests are vendored with checksums and source URLs. Setup preloads the Volcano and worker images through Docker using an archive for the runtime architecture. The setup script uses a dedicated `scholarly-prototype` kind cluster and `.runtime/kubeconfig`. On macOS it starts an isolated 4-CPU, 8-GiB Colima profile without host directory mounts or switching the default Docker context. The virtual disk has a 30-GiB allocation ceiling. Setup downloads public images and binaries; no cloud account or GPU is required.

The cluster runner requires an empty `prototype` namespace. It cleans up its Jobs after successful scenarios. On failure it stops its bridge but preserves cluster objects for diagnosis. To clean only this dedicated namespace's test Jobs before retrying:

```sh
.tools/kubectl --kubeconfig .runtime/kubeconfig --context kind-scholarly-prototype delete jobs --all -n prototype --wait=true
```

Do not point the scripts at an existing production/shared cluster. The prototype intentionally uses the dedicated cluster's admin credentials; tenant RBAC, certificate management and webhook authorization are not implemented.

## Files

- `src/bridge.py`: live AdmissionCheck controller, idempotent PodGroup creation, attempt tokens, optimistic concurrency and placement timeout.
- `scripts/run_cluster.py`: nine live integration checks plus quota-release observations; records API watches and snapshots.
- `src/checkpoint_worker.py`: two-process PyTorch training and manifest commit/recovery.
- `scripts/run_checkpoint.py`: six recovery scenarios and three negative controls across three seeds.
- `scripts/configure_cluster.py` and `config/policies.yaml`: concrete quota/flavor/PriorityClass/Volcano configuration.
- `scripts/setup_cluster.sh`: isolated cluster setup using pinned upstream manifests.
- `results/cluster-final/`, `results/checkpoint-final/`: designated measured runs; `results/cluster-reproduced/` records the fresh-cluster regression run.
- `results/environment.json`: runtime versions, controller image digests and source hashes.
- `results/development/`: earlier setup/debug runs; not counted as final evidence.

## Important implementation limits

The controller is single-process, polling-based, and lacks leader election, a validating pod webhook, image attestations, metrics export, general workload adapters, and an automatic heartbeat that changes AdmissionCheck Active when it stops. Existing Ready Jobs can continue if the bridge stops. Cancellation is tested via Workload deactivation; direct deletion and unreachable-node fencing are outside the tested protocol. Readmission tokens use the check's reset-to-Pending behavior of the pinned Kueue version; this is not an upstream generic attempt-ID field. The controller refuses to recompute Ready from cached status after a conflict.

Kueue quota release and physical pod termination are distinct events. This artifact records that distinction; it does not prove the draft's stronger invariant that every terminating worker retains quota until physical exit. Volcano observes physical occupancy independently. Do not claim atomic binding or a strict reclaim deadline from these tests.

The checkpoint test uses two real local CPU processes and a shared POSIX filesystem, with full model/optimizer state in each rank's shard. It is not a four-rank simulation, an object-store implementation, a sharded optimizer, or a PyTorch Distributed Checkpoint implementation. Interruptions are injected at a synchronized training-step boundary. Bit-exact results apply to this deterministic CPU setup.

## Stop and remove the experiment environment

Stop the VM to release CPU/RAM while preserving the cluster and image cache:

```sh
colima --profile scholarly-prototype stop
```

Resume it with the same setup script. To remove only the created kind cluster:

```sh
DOCKER_CONTEXT=colima-scholarly-prototype kind delete cluster --name scholarly-prototype
```

The project-local `.venv`, `.tools`, and `.runtime` directories are excluded from Git. `.runtime/kubeconfig` contains credentials and must never be published. No manuscript or response-letter results are updated automatically; report only the observations in the completed result files.

## Verify or package retained evidence

```sh
.venv/bin/python scripts/verify_results.py
.venv/bin/python scripts/package_evidence.py
```

The first command checks the recorded result summaries. The second builds `../prototype-evidence.zip`, including raw evidence and checkpoint tensors, and verifies every archived file against `SHA256SUMS.json`. Tool installations and runtime credentials are excluded. Checkpoint tensors are intentionally excluded from Git by `.gitignore` but included in the local evidence archive.
