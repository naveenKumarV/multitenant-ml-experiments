# Narrow prototype test specification

The prototype validates the boundary between Kueue admission and Volcano placement, quota behavior, and a separate distributed CPU recovery protocol. It is a research implementation, not a deployed ML platform. These scenarios were selected to address Reviewer 1; development/debug runs are retained separately from the final runs.

## Live Kubernetes experiments

Environment: Kubernetes 1.33.1, Kueue 0.17.0, Volcano 1.13.0, two worker nodes, two synthetic `example.com/accelerator` units per worker. Each Indexed Job has two pods, one unit per pod. Prod and Dev each have nominal quota two; Prod may borrow two, Dev may lend two and reclaim with `Any`. The default Kubernetes scheduler has non-preempting PriorityClasses; Volcano runs only enqueue and allocate. Workloads use a single Volcano queue capped at the four available synthetic units.

1. Stop the bridge, submit a suspended Job, and observe quota reserved with no admission and no pods.
2. Start the bridge and verify two workers run with the assigned flavor node selector, matching node labels, and the current attempt's PodGroup annotations.
3. Deactivate the Workload and observe suspension/drain and eventual quota release. Record pods remaining immediately after quota release; do not assume simultaneous transitions.
4. Pause after durable PodGroup creation, kill the bridge process, restart, and verify that the same PodGroup UID is reused and the Job runs.
5. Save an old Workload object, deactivate/reactivate, and submit the old status write. Require HTTP 409 and a distinct subsequent attempt token.
6. Submit a physically unplaceable gang. Require bridge Retry, Kueue eviction, and an observed release of quota in the API watch history. Deactivate after that observation to terminate repeated retries.
7. Fill both nominal quotas; observe for six seconds that a new Prod Job remains suspended while both existing Jobs keep running. This finite observation does not prove an unbounded waiting guarantee.
8. Use Prod's idle borrowing allowance to run four units while Dev is idle.
9. Submit Dev demand; require a Prod borrower to be preempted, Dev to run on its two reclaimed units, and one two-unit Prod Job to remain running.

Raw artifacts: workload watch events, cross-object polling snapshots, bridge JSONL events, Kubernetes events, submitted Jobs and process logs. API watches capture transitions that polling can miss. Snapshot reads across different objects are not atomic, and neither trace coverage nor passing scenarios constitute exhaustive concurrency verification.

## Two-process PyTorch checkpoint experiments

Two CPU processes communicate using Gloo and synchronously average gradients. The network has Linear(4,8), Tanh, Dropout(0.25), Linear(8,1), with Adam and float64 parameters. Each rank reads a shuffled disjoint partition of a fixed synthetic dataset. Seeds: 7, 19, 41. An uninterrupted reference trains 60 steps. Periodic checkpoints commit every 20 steps; faults/interruption occur at step 35.

For each seed, run six positive scenarios: cooperative SIGTERM, abrupt SIGKILL, loss of rank 1 before writing, corruption of rank 1's shard, coordinator exit before manifest publication, and coordinator exit after manifest but before pointer publication. The expected resume point is step 35 for successful SIGTERM and post-manifest coordinator loss; it is step 20 for the other four cases.

Every resumed run must match the baseline exactly for model parameters, Adam state, Torch/Python RNG state, data order/cursor, and every resumed sample-ID/loss trace. Check that the latest pointer refers to a complete, hash-verified manifest. Three negative controls per seed omit RNG, iterator, or optimizer state; each must change the final model.

Checkpoint storage is a shared local POSIX filesystem. Each rank publishes its own shard; rank 0 verifies all shard hashes, publishes an immutable manifest, and then advances a pointer. Resume scans verified manifests rather than trusting the pointer. Coordinator/rank failure is a real process exit. A torn write is deliberately injected file corruption. This does not test object-store consistency, remote filesystem failure, actual machine power loss, GPUs, PyTorch Distributed Checkpoint, or sharded optimizers.

## Claims explicitly excluded

No 912-state model check, no 200-trial/510-interruption study, no production throughput, no cost savings, no DCGM accuracy, no hardware topology guarantee, no atomic multi-pod Kubernetes binding, no all-workload admission security, and no full-platform correctness proof. The CPU recovery and scheduler tests are separate: training does not run inside the scheduler test pods.
