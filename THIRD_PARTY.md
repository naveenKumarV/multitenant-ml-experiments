# Third-party manifests

The unmodified upstream installation manifests in `prototype/vendor/` are retained with their download URLs and SHA-256 digests in `prototype/vendor/manifest.json`.

- Kueue v0.17.0, Kubernetes SIGs: [upstream](https://github.com/kubernetes-sigs/kueue/tree/v0.17.0), [Apache-2.0 license](licenses/kueue-LICENSE).
- Volcano v1.13.0, Volcano authors: [upstream](https://github.com/volcano-sh/volcano/tree/v1.13.0), [Apache-2.0 license](licenses/volcano-LICENSE).

Python dependency versions are pinned in `prototype/requirements.lock`; those dependencies and container images are fetched during setup and retain their respective upstream licenses. The bundled licenses cover the upstream projects and do not imply a new license grant for original prototype code.
