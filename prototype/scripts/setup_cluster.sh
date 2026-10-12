#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$PWD/.tools:/opt/homebrew/bin:$PATH"
mkdir -p .runtime .tools
# The profile and kubeconfig are dedicated to this experiment.
if [[ "$(uname -s)" == Darwin ]]; then
  export DOCKER_CONTEXT=colima-scholarly-prototype
  colima start scholarly-prototype --cpu 4 --memory 8 --disk 30 --mount none --activate=false --ssh-config=false
fi
if [[ ! -x .tools/kubectl ]]; then
  os=$(uname -s | tr '[:upper:]' '[:lower:]')
  arch=$(uname -m)
  [[ "$arch" != aarch64 ]] || arch=arm64
  [[ "$arch" != x86_64 ]] || arch=amd64
  curl -fsSL "https://dl.k8s.io/release/v1.33.5/bin/$os/$arch/kubectl" -o .tools/kubectl
  curl -fsSL "https://dl.k8s.io/release/v1.33.5/bin/$os/$arch/kubectl.sha256" -o .tools/kubectl.sha256
  .venv/bin/python -c 'import hashlib,pathlib; p=pathlib.Path(".tools/kubectl"); assert hashlib.sha256(p.read_bytes()).hexdigest()==pathlib.Path(str(p)+".sha256").read_text().strip()'
  chmod +x .tools/kubectl
fi
if ! kind get clusters | grep -qx scholarly-prototype; then
  kind create cluster --name scholarly-prototype --image kindest/node:v1.33.1 --config config/kind.yaml --kubeconfig .runtime/kubeconfig --wait 120s
else
  kind export kubeconfig --name scholarly-prototype --kubeconfig .runtime/kubeconfig
fi
k() { kubectl --kubeconfig "$PWD/.runtime/kubeconfig" --context kind-scholarly-prototype "$@"; }
# Preload through the host runtime so nested-node registry timeouts do not
# strand the webhook init Job. Cached image layers are reused on later setups.
images=(docker.io/volcanosh/vc-webhook-manager:v1.13.0 docker.io/volcanosh/vc-controller-manager:v1.13.0 docker.io/volcanosh/vc-scheduler:v1.13.0 docker.io/library/busybox:1.37.0)
for image in "${images[@]}"; do
  docker pull "$image"
done
image_platform=$(docker info --format '{{.OSType}}/{{.Architecture}}' | sed 's/aarch64/arm64/; s/x86_64/amd64/')
# Export only the runtime architecture: Docker's multi-platform image index can
# otherwise reference layers absent from a local pull during kind import.
docker image save --platform="$image_platform" -o .runtime/workload-images.tar "${images[@]}"
kind load image-archive --name scholarly-prototype .runtime/workload-images.tar
.venv/bin/python scripts/verify_vendor.py
k apply --server-side -f vendor/kueue-v0.17.0.yaml
k rollout status deployment/kueue-controller-manager -n kueue-system --timeout=180s
k apply --server-side -f vendor/volcano-v1.13.0.yaml
k rollout status deployment/volcano-admission -n volcano-system --timeout=180s
k rollout status deployment/volcano-scheduler -n volcano-system --timeout=180s
.venv/bin/python scripts/configure_cluster.py
k rollout status deployment/volcano-scheduler -n volcano-system --timeout=120s
