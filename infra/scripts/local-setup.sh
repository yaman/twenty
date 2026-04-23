#!/usr/bin/env bash
# Deploys the full Twenty scalability stack on local Kubernetes (Docker Desktop)
#
# Usage:
#   bash infra/scripts/local-setup.sh          # Deploy everything
#   bash infra/scripts/local-setup.sh --down    # Tear down everything

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$INFRA_DIR/.." && pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[+]${NC} $1"; }
warn() { echo -e "${YELLOW}[!]${NC} $1"; }
err() { echo -e "${RED}[x]${NC} $1"; exit 1; }

wait_for_pods() {
  local namespace=$1
  local label=$2
  local timeout=${3:-300}
  log "Waiting for pods ($label) in $namespace (timeout: ${timeout}s)..."
  kubectl wait --for=condition=ready pod -l "$label" -n "$namespace" --timeout="${timeout}s" 2>/dev/null || {
    warn "Pods not ready after ${timeout}s. Checking status..."
    kubectl get pods -l "$label" -n "$namespace"
    return 1
  }
}

teardown() {
  log "Tearing down Twenty infrastructure..."
  helm uninstall twenty -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/sql/optimization-job.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/sql/mv-refresh-cronjob.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/monitoring/prometheusrules.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/monitoring/dashboards/" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/monitoring/servicemonitor-twenty-server.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/monitoring/servicemonitor-twenty-worker.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/monitoring/servicemonitor-keydb.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/twenty/service-server-metrics.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/twenty/service-worker-metrics.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/twenty/hpa-server.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/twenty/hpa-worker.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/twenty/pdb-server.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/twenty/pdb-worker.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/keydb/" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/database/scheduled-backup.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/database/objectstore-local.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/database/pooler-rw.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/database/pooler-ro.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/database/monitoring-queries.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  kubectl delete -f "$INFRA_DIR/database/cluster.yaml" --ignore-not-found -n twenty 2>/dev/null || true
  helm uninstall minio -n minio 2>/dev/null || true
  helm uninstall monitoring -n monitoring 2>/dev/null || true
  kubectl delete -f https://github.com/cloudnative-pg/plugin-barman-cloud/releases/download/v0.12.0/manifest.yaml --ignore-not-found 2>/dev/null || true
  helm uninstall cnpg-operator -n cnpg-system 2>/dev/null || true
  helm uninstall cert-manager -n cert-manager 2>/dev/null || true
  log "Teardown complete."
  exit 0
}

if [[ "${1:-}" == "--down" ]]; then
  teardown
fi

# Preflight
command -v kubectl >/dev/null || err "kubectl not found"
command -v helm >/dev/null || err "helm not found"
kubectl cluster-info >/dev/null 2>&1 || err "No Kubernetes cluster available"

# Add Helm repos
log "Adding Helm repositories..."
helm repo add cnpg https://cloudnative-pg.github.io/charts 2>/dev/null || true
helm repo add minio https://charts.min.io/ 2>/dev/null || true
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 2>/dev/null || true
helm repo update

# Create namespaces
log "Creating namespaces..."
kubectl create namespace twenty --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace cnpg-system --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace minio --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -

# === Wave 0: Operators ===
log "=== Wave 0: Installing operators ==="

log "Installing cert-manager (required by Barman Cloud Plugin)..."
helm repo add jetstack https://charts.jetstack.io 2>/dev/null || true
helm repo update jetstack
helm upgrade --install cert-manager jetstack/cert-manager \
  -n cert-manager --create-namespace \
  --set crds.enabled=true \
  --wait --timeout 5m

log "Installing CloudNativePG operator..."
helm upgrade --install cnpg-operator cnpg/cloudnative-pg \
  -n cnpg-system \
  -f "$INFRA_DIR/operators/cnpg-values.yaml" \
  --wait --timeout 5m

log "Installing Barman Cloud Plugin..."
kubectl apply --server-side \
  -f https://github.com/cloudnative-pg/plugin-barman-cloud/releases/download/v0.12.0/manifest.yaml
kubectl -n cnpg-system wait --for=condition=available deployment/barman-cloud --timeout=120s

log "Installing kube-prometheus-stack..."
helm upgrade --install monitoring prometheus-community/kube-prometheus-stack \
  -n monitoring \
  -f "$INFRA_DIR/monitoring/kube-prometheus-stack-values.yaml" \
  --wait --timeout 10m

# === Wave 1: Data Layer ===
log "=== Wave 1: Deploying data layer ==="

log "Installing MinIO..."
helm upgrade --install minio minio/minio \
  -n minio \
  -f "$INFRA_DIR/minio/values-local.yaml" \
  --wait --timeout 5m

log "Creating MinIO credentials secret for CNPG..."
kubectl create secret generic minio-creds \
  --namespace=twenty \
  --from-literal=ACCESS_KEY_ID=minioadmin \
  --from-literal=SECRET_ACCESS_KEY=minioadmin123 \
  --dry-run=client -o yaml | kubectl apply -f -

log "Deploying backup ObjectStore (must exist before cluster for Barman Cloud Plugin)..."
kubectl apply -f "$INFRA_DIR/database/objectstore-local.yaml"

log "Deploying CloudNativePG Cluster..."
kubectl apply -f "$INFRA_DIR/database/monitoring-queries.yaml"
kubectl apply -f "$INFRA_DIR/database/cluster.yaml"

log "Waiting for CNPG cluster to be ready (this takes 2-5 minutes)..."
kubectl wait --for=condition=Ready cluster/twenty-db -n twenty --timeout=600s 2>/dev/null || {
  warn "Cluster not ready via condition check. Checking pods..."
  sleep 30
  kubectl get pods -n twenty -l cnpg.io/cluster=twenty-db
}

log "Deploying PgBouncer poolers..."
kubectl apply -f "$INFRA_DIR/database/pooler-rw.yaml"
kubectl apply -f "$INFRA_DIR/database/pooler-ro.yaml"
sleep 10

log "Deploying scheduled backup..."
kubectl apply -f "$INFRA_DIR/database/scheduled-backup.yaml"

log "Deploying KeyDB..."
kubectl apply -f "$INFRA_DIR/keydb/configmap.yaml"
kubectl apply -f "$INFRA_DIR/keydb/service-headless.yaml"
kubectl apply -f "$INFRA_DIR/keydb/service.yaml"
kubectl apply -f "$INFRA_DIR/keydb/statefulset.yaml"
kubectl apply -f "$INFRA_DIR/keydb/pdb.yaml"
wait_for_pods twenty "app.kubernetes.io/name=twenty-keydb" 120

# === Wave 2: Application Layer ===
log "=== Wave 2: Deploying Twenty ==="

helm upgrade --install twenty "$REPO_ROOT/packages/twenty-docker/helm/twenty" \
  -n twenty \
  -f "$INFRA_DIR/twenty/values-local.yaml" \
  --wait --timeout 10m

log "Applying HPA and PDB manifests..."
kubectl apply -f "$INFRA_DIR/twenty/hpa-server.yaml"
kubectl apply -f "$INFRA_DIR/twenty/hpa-worker.yaml"
kubectl apply -f "$INFRA_DIR/twenty/pdb-server.yaml"
kubectl apply -f "$INFRA_DIR/twenty/pdb-worker.yaml"

# === Wave 3: Observability ===
log "=== Wave 3: Deploying observability ==="

kubectl apply -f "$INFRA_DIR/twenty/service-server-metrics.yaml"
kubectl apply -f "$INFRA_DIR/twenty/service-worker-metrics.yaml"
kubectl apply -f "$INFRA_DIR/monitoring/servicemonitor-twenty-server.yaml"
kubectl apply -f "$INFRA_DIR/monitoring/servicemonitor-twenty-worker.yaml"
kubectl apply -f "$INFRA_DIR/monitoring/servicemonitor-keydb.yaml"
kubectl apply -f "$INFRA_DIR/monitoring/prometheusrules.yaml"
kubectl apply -f "$INFRA_DIR/monitoring/dashboards/"

# === Post-deployment ===
log "=== Running SQL optimizations ==="

log "Waiting for Twenty to initialize database..."
sleep 30

kubectl delete job twenty-sql-optimizations -n twenty --ignore-not-found 2>/dev/null || true
kubectl apply -f "$INFRA_DIR/sql/optimization-job.yaml"
kubectl wait --for=condition=complete job/twenty-sql-optimizations -n twenty --timeout=300s || {
  warn "SQL optimization job may have issues. Check logs:"
  echo "  kubectl logs job/twenty-sql-optimizations -n twenty"
}

kubectl apply -f "$INFRA_DIR/sql/mv-refresh-cronjob.yaml"

# === Summary ===
log "=== Deployment Complete ==="
echo ""
echo "Services:"
echo "  Twenty:    http://localhost:$(kubectl get svc twenty-server -n twenty -o jsonpath='{.spec.ports[0].port}' 2>/dev/null || echo '3000')"
echo "  Grafana:   http://localhost:30300 (admin/admin)"
echo ""
echo "Useful commands:"
echo "  kubectl get pods -n twenty"
echo "  kubectl get cluster -n twenty"
echo "  kubectl logs -f deploy/twenty-server -n twenty"
echo "  kubectl logs job/twenty-sql-optimizations -n twenty"
