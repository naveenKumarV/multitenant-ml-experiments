# Narrow prototype: measured results and limits

Executed locally on 12 October 2026. This report covers a working research implementation and its retained execution evidence. It replaces none of the manuscript automatically.

## Outcome

The live Kubernetes run in `results/cluster-final/results.json` passed all nine specified checks. The CPU training run in `results/checkpoint-final/results.json` completed 18 interrupted/resumed comparisons across three seeds; every comparison matched its uninterrupted reference exactly. All nine state-omission negative controls diverged as expected. These are small correctness experiments, not production performance measurements or exhaustive proofs.

The scheduler and checkpoint experiments are separate. Kubernetes workers execute a shell workload with a two-second simulated termination flush. The real PyTorch training runs as two host CPU processes; it was not submitted through Kueue or Volcano.

## Live admission and quota behavior

The cluster has one control-plane node and two worker nodes, running Kubernetes 1.33.1, Kueue 0.17.0 and Volcano 1.13.0. Each worker advertises two synthetic extended-resource units, for four total; there are no physical GPUs. Prod and Dev each own two nominal units. Prod can borrow two from Dev; Dev can reclaim its lent quota even though Prod has a higher workload priority. A single Volcano queue covers the four units, with only enqueue and allocate actions enabled.

The Python bridge persists an attempt token in the Workload admission-check status before preparing its PodGroup. It publishes Ready through a resourceVersion-conditional status update and supplies the group annotation through Kueue's pod-set updates. Ready expresses that the placement contract is prepared, not that nodes have already been reserved. This avoids waiting for pods that Kueue will only create after admission.

| Check | Observed result |
|---|---|
| Reservation before admission | Quota reserved while the Job remained suspended with zero pods. |
| Admission and flavor propagation | Two workers ran with the assigned flavor selector, matching node labels, current attempt token and group annotation. |
| Cancellation | Workload deactivation eventually removed both pods and cleared the reservation. |
| Bridge crash after PodGroup creation | Restart reused the same PodGroup UID and the Job ran. |
| Stale Ready after readmission | The saved old status write received HTTP 409; the new admission used a different attempt token. |
| Impossible placement | The bridge requested Retry after its timeout; Kueue evicted the workload and a quota-release transition was captured by the API watch. |
| Saturation | With both nominal allocations occupied, an additional Prod Job had no pods or reservation during a six-second observation. |
| Borrowing | Prod ran four synthetic units while Dev was idle. |
| Lender reclaim | Dev's demand preempted one two-unit Prod Job; Dev and the remaining Prod Job each ran two workers. |

These nine checks are scenarios in one sequence, not nine independent statistical replications. The saturation observation does not establish an unbounded waiting guarantee. The placement timeout is a request to Kueue, not a guarantee of a fixed physical shutdown deadline. Kubernetes API snapshots read several objects separately and are not atomic.

### Finding that changes the manuscript's invariant

Quota release and physical termination must be represented separately. In the designated run, nine of twelve recorded drain observations showed two pods still in API phase `Running`, with deletion timestamps, after the Workload no longer had `QuotaReserved=True`. Some drain observations concern the same Job at different times; this count is descriptive, not a failure rate. Pod phase alone does not prove that its process was still executing at the exact observation time.

Consequently, this implementation does **not** establish that a terminating worker retains logical quota until physical exit. The paper must not present that stronger statement as verified. Node resource accounting and Volcano placement are a separate safeguard, and their safety under every shutdown interleaving is also not proved here. A strict reservation-until-exit protocol would require further implementation and tests.

This observation is consistent with the pinned Kueue source: the eviction path clears the reservation after its Job adapter reports no active pods; that adapter checks `Job.status.active`. See the [v0.17.0 job reconciler](https://github.com/kubernetes-sigs/kueue/blob/v0.17.0/pkg/controller/jobframework/reconciler.go) and [Job adapter](https://github.com/kubernetes-sigs/kueue/blob/v0.17.0/pkg/controller/jobs/job/job_controller.go). Terminating pods and active Job pod counts need not change simultaneously.

## Real two-process checkpoint recovery

PyTorch 2.9.1 ran two CPU processes using Gloo gradient averaging, float64 parameters, a small neural network with dropout, and Adam. Each rank used a shuffled disjoint partition of a fixed synthetic dataset. Seeds were 7, 19 and 41. Each reference ran 60 steps; periodic checkpoints were written every 20 steps; each interruption was injected at a synchronized boundary after step 35.

Each rank writes its own model, optimizer, Torch/Python RNG and data-iterator state. The coordinator verifies shard hashes, publishes a manifest with fsync and rename, then updates a convenience pointer. Recovery scans verified manifests, so a stale pointer does not hide a newer complete checkpoint. Storage is a shared local POSIX filesystem.

| Fault at step 35 | Resume step in all three seeds | Steps recomputed | Exact recovery |
|---|---:|---:|---:|
| SIGTERM delivered to both worker processes | 35 | 0 | 3/3 |
| Abrupt SIGKILL | 20 | 15 | 3/3 |
| Rank 1 killed before its shard write | 20 | 15 | 3/3 |
| Rank 1 shard deliberately corrupted | 20 | 15 | 3/3 |
| Coordinator exits before manifest publication | 20 | 15 | 3/3 |
| Coordinator exits after manifest, before pointer | 35 | 0 | 3/3 |

“Exact recovery” means equality of final model parameters, optimizer state, RNG state, iterator state and step number on both ranks, plus equality of every resumed sample-ID/loss trace to the corresponding reference suffix. It does not mean that work after the previous committed checkpoint was never lost.

Three negative controls per seed omit RNG, iterator or optimizer state at resume. Each of the nine controls changed the final model, demonstrating that the comparison detects these omissions.

Measured SIGTERM-to-launcher-exit durations were 0.435, 0.385 and 0.441 seconds. They describe three coordinated local CPU runs with tiny checkpoints; they are neither a production grace-period recommendation nor a checkpoint throughput benchmark. Faults were injected at a safe step boundary, not at arbitrary instructions. File corruption was deliberate; power loss and remote-storage failures were not tested.

## Evidence and reproducibility

Start with `README.md` and `TEST_PLAN.md`. Both runners reject an existing output directory. Raw data include submitted Jobs, Workload watch events, cross-object snapshots, bridge decisions, Kubernetes Events, process commands and exit codes, per-rank traces, checkpoints and exact-comparison outputs. Runtime versions and controller image IDs are recorded in `results/environment.json`.

The earlier `cluster-final` run retains its original source hashes in `results/cluster-final/environment.json`. After deleting and recreating the dedicated cluster, setup was verified and all nine checks passed again in `results/cluster-reproduced/results.json`. This regression run is recorded separately rather than added to the original scenario count. Reapplying the setup script to the existing cluster also succeeded (`results/setup-repeat.log`). The final source also checks the fresh Workload's active/Pending status before Ready and explicitly selects the dedicated context.

Development runs are retained under `results/development/`. They include an initial PyTorch rendezvous failure fixed by explicit loopback IPv4, and an initial cluster observer failure caused by polling missing a short quota-release transition, fixed by preserving a Workload API watch. Fresh-cluster reproduction also exposed nested-node Docker Hub download timeouts and a multi-platform image-import error. Setup now preloads an archive limited to the runtime architecture; the unsuccessful setup logs are retained alongside the successful setup log. Passing development runs are not included in the reported totals. The test specification was refined during development; this is not a formally preregistered study.

## Limits on claims

This is a single-process Python bridge for fixed-size batch Jobs. It has no leader election, tenant-security enforcement webhook, production RBAC deployment, automatic Active heartbeat, general workload adapters, GPU device plugin, topology validation, or sharded-optimizer/object-store implementation. The training uses manually averaged gradients; it does not use the PyTorch Distributed Checkpoint API. Direct deletion, unreachable-node fencing, malicious tenants, concurrent controllers and arbitrary failure interleavings are not covered.

Do not carry forward the earlier response's unsupported 912-state, 200-trial or 510-interruption figures, or describe this controller as Go. Do not claim production cost, utilization or latency improvements, atomic multi-pod binding, or a full platform proof. This evidence supports a narrow executable architecture example and a real interrupted/resumed training example, with the quota/termination caveat made explicit.

## Suggested factual wording for the revision

> We implemented a single-process Python admission bridge and evaluated nine live integration checks on Kubernetes 1.33.1 with Kueue 0.17.0 and Volcano 1.13.0, using four synthetic extended-resource units across two worker nodes. The checks exercised admission gating, flavor and PodGroup propagation, controller restart, stale-status rejection, placement failure, cancellation, quota borrowing and lender reclaim. Separately, a two-process CPU PyTorch experiment evaluated six checkpoint interruption scenarios across three seeds. All 18 resumed runs matched their uninterrupted references in final training state and continuation traces; nine state-omission controls diverged. These experiments validate the exercised paths only. In particular, logical quota release can precede the disappearance of terminating pods, and no reservation-until-physical-exit invariant or production performance result is claimed.
