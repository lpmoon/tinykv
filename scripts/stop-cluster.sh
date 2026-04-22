#!/bin/bash
#
# Stop the TinyKV cluster
#
# Usage: ./scripts/stop-cluster.sh [cluster-name]
#

set -e

CLUSTER_NAME=${1:-test-cluster}
COORDINATOR_PORT=8000
NODE1_PORT=7000
NODE2_PORT=7001
NODE3_PORT=7002
NODE4_PORT=7003

echo "Stopping TinyKV Cluster: ${CLUSTER_NAME}"

for port in ${COORDINATOR_PORT} ${NODE1_PORT} ${NODE2_PORT} ${NODE3_PORT} ${NODE4_PORT}; do
    pid=$(lsof -ti:${port} 2>/dev/null || true)
    if [ -n "${pid}" ]; then
        echo "Stopping process on port ${port}: ${pid}"
        kill ${pid} 2>/dev/null || true
    else
        echo "No process on port ${port}"
    fi
done

echo "Done."
