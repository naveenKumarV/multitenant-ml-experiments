# Multi-tenant admission and checkpoint experiments

This standalone repository preserves the source and measured evidence from the local experiments executed on **12 October 2026**. It accompanies “A Closed-Loop Control-Plane Architecture for Multi-Tenant ML Platform Scheduling and Cost Attribution on Kubernetes.” This repository contains only the research artifact and has a fresh Git history, independent of the broader ML platform repository.

- [Measured results and limitations](prototype/REPORT.md)
- [Reproduction instructions](prototype/README.md)
- [Scenario definitions](prototype/TEST_PLAN.md)
- [Live Kubernetes results](prototype/results/cluster-final/results.json) and [fresh-cluster repetition](prototype/results/cluster-reproduced/results.json)
- [Two-process PyTorch recovery results](prototype/results/checkpoint-final/results.json)
- [Complete original evidence archive](evidence/prototype-evidence-2026-10-12.zip), including checkpoint tensors and development runs

Nine live Kubernetes checks passed and passed again after rebuilding the cluster. Separately, 18 interrupted/resumed CPU training runs across three seeds exactly matched their uninterrupted references; nine state-omission controls diverged as expected. The scheduler uses synthetic accelerator resources; the PyTorch training uses two actual host CPU processes. These are separate experiments, with no physical GPUs or production performance claim.

A material limitation is retained: Kueue quota release can occur while terminating pods remain visible. The experiments do not establish a reservation-until-physical-exit invariant or exhaustive concurrency correctness.

## Verify the retained evidence

From this directory, with Python 3.12:

```sh
python3 verify_archive.py
python3 prototype/scripts/verify_results.py
python3 prototype/scripts/verify_vendor.py
```

These commands verify archive integrity, retained result-summary consistency, and upstream manifest checksums. They do not rerun the experiments; use the reproduction instructions to do that.

The exact original ZIP has SHA-256:

```text
41da3fbdbf3da180fdfa51611fca72d9f3c0024beb7735f6807aa454f4885fc6
```

The archive contains its own `prototype/SHA256SUMS.json`, covering 1,207 source and evidence files. The verifier also requires every tracked/extracted source and evidence file to match that snapshot. Checkpoint `.pt` files and checkpoint directories are deliberately excluded from the readable Git tree, but are included in the ZIP. To obtain the complete snapshot, extract the ZIP into a new directory; checkpoint files should only be loaded from a trusted artifact.

Raw commands retain the original local execution paths and timestamps for provenance. Those paths describe the original machine; use relative commands in the reproduction instructions on a different machine. Historical source hashes remain unchanged even where the source was refined between the designated run and its subsequent regression run.

For a stable scholarly citation, use this repository's URL at the specific Git commit that added the evidence, rather than a moving branch URL. GitHub preserves the files and recorded results; uploading them is not an independent replication. Repository visibility determines who can access the evidence.

Upstream manifest provenance and licenses are described in [THIRD_PARTY.md](THIRD_PARTY.md). Runtime credentials, virtual machines, tool installations, manuscript files and reviewer correspondence are not part of this artifact.
