#!/usr/bin/env bash
# Builds the benchmark image, deploys it to the current kubectl context and runs the driver for each replica count.
# Prerequisites (see ../README.md): namespace "bench", nodes labelled eventlog-bench=node (one per replica) and
# eventlog-bench=driver, an ACR attached to the cluster, a storage account and a workload identity with Storage Blob
# Data Contributor federated to system:serviceaccount:bench:elb.
# Usage: ACR=<registry> BLOB_ENDPOINT=https://<account>.blob.core.windows.net CLIENT_ID=<identity client id> \
#        ./run.sh [seconds] [suite] [replica counts...]
set -euo pipefail
SECONDS_PER_SCENARIO=${1:-20}
SUITE=${2:-full}
shift 2 2>/dev/null || shift $#
REPLICAS_LIST=${*:-1 2 3 4}
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
WORK=${WORK:-$(mktemp -d)}
OUT=${OUT:-$HERE/results}
TAG=${TAG:-$(date +%Y%m%d%H%M%S)}
mkdir -p "$WORK/image" "$OUT"

if ! kubectl -n bench get secret elb-tls >/dev/null 2>&1; then
    # A private CA trusted only by the image signs the peer certificate for the headless service names.
    openssl req -x509 -newkey rsa:2048 -nodes -keyout "$WORK/ca.key" -out "$WORK/ca.crt" -days 30 -subj "/CN=elb-bench-ca"
    openssl req -newkey rsa:2048 -nodes -keyout "$WORK/tls.key" -out "$WORK/tls.csr" -subj "/CN=elb"
    printf "subjectAltName=DNS:*.elb.bench.svc.cluster.local,DNS:*.elb.bench.svc,DNS:*.elb\nextendedKeyUsage=serverAuth\n" > "$WORK/ext.cnf"
    openssl x509 -req -in "$WORK/tls.csr" -CA "$WORK/ca.crt" -CAkey "$WORK/ca.key" -CAcreateserial -out "$WORK/tls.crt" -days 30 -extfile "$WORK/ext.cnf"
    kubectl -n bench create secret tls elb-tls --cert="$WORK/tls.crt" --key="$WORK/tls.key"
    kubectl -n bench create configmap elb-ca --from-file=ca.crt="$WORK/ca.crt"
    kubectl -n bench create secret generic elb-api --from-literal=key="$(openssl rand -hex 24)"
fi
kubectl -n bench get configmap elb-ca -o jsonpath='{.data.ca\.crt}' > "$WORK/image/ca.crt"

dotnet publish "$ROOT/EventLogBench/EventLogBench.csproj" -c Release -r linux-x64 --self-contained false -o "$WORK/image/app"
cp "$HERE/Dockerfile" "$WORK/image/"
az acr build -r "$ACR" -t "elb:$TAG" "$WORK/image"

export IMAGE="$ACR.azurecr.io/elb:$TAG" CLIENT_ID BLOB_ENDPOINT COMMIT_LINGER_US="${COMMIT_LINGER_US:-}" \
    ADAPTIVE_LINGER_MAX_US="${ADAPTIVE_LINGER_MAX_US:-}"
for REPLICAS in $REPLICAS_LIST; do
    export REPLICAS STORAGE_PREFIX="run-$TAG-n$REPLICAS"
    kubectl -n bench delete statefulset elb --ignore-not-found --wait=true
    kubectl -n bench wait --for=delete pod -l app=elb --timeout=120s || true
    envsubst < "$HERE/bench.yaml" | kubectl apply -f -
    kubectl -n bench rollout status statefulset/elb --timeout=300s
    kubectl -n bench rollout status deploy/elb-driver --timeout=300s
    DRIVER=$(kubectl -n bench get pods -l app=elb-driver \
        -o go-template='{{range .items}}{{if not .metadata.deletionTimestamp}}{{.metadata.name}}{{"\n"}}{{end}}{{end}}' | head -1)
    kubectl -n bench exec "$DRIVER" -- dotnet EventLogBench.dll driver --nodes "$REPLICAS" \
        --seconds "$SECONDS_PER_SCENARIO" --suite "$SUITE" --out /tmp/results.json
    kubectl -n bench cp "$DRIVER:/tmp/results.json" "$OUT/n$REPLICAS.json"
done
echo "Results in $OUT"
