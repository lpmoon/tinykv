#!/bin/bash
#
# Start a 3-node TinyKV cluster with a Coordinator
#
# Usage:
#   ./scripts/start-cluster.sh [cluster-name]   # Start with existing data
#   ./scripts/start-cluster.sh [cluster-name] --clean   # Start fresh (delete data)
#

set -e

CLEAN_DATA=false

# Parse arguments
CLUSTER_NAME=""
for arg in "$@"; do
    if [[ "$arg" == "--clean" ]]; then
        CLEAN_DATA=true
    elif [[ "$arg" != "--"* ]]; then
        CLUSTER_NAME="$arg"
    fi
done

CLUSTER_NAME=${CLUSTER_NAME:-test-cluster}
BASE_DIR="/tmp/tinykv-${CLUSTER_NAME}"
COORDINATOR_PORT=8000
NODE1_PORT=7000
NODE2_PORT=7001
NODE3_PORT=7002
NODE4_PORT=7003

JAR_FILE="target/tinykv-1.0-SNAPSHOT.jar"

echo "Starting TinyKV Cluster: ${CLUSTER_NAME}"
echo "=========================================="

# Create data directories
if [ "$CLEAN_DATA" = true ]; then
    echo "WARNING: Cleaning existing data..."
    rm -rf ${BASE_DIR}
fi
mkdir -p ${BASE_DIR}/node1
mkdir -p ${BASE_DIR}/node2
mkdir -p ${BASE_DIR}/node3
mkdir -p ${BASE_DIR}/node4

# Build the JAR if not exists
if [ ! -f "${JAR_FILE}" ]; then
    echo "Building JAR..."
    mvn package -DskipTests -q
fi

# Kill any existing processes on these ports
for port in ${COORDINATOR_PORT} ${NODE1_PORT} ${NODE2_PORT} ${NODE3_PORT} ${NODE4_PORT}; do
    pid=$(lsof -ti:${port} 2>/dev/null || true)
    if [ -n "${pid}" ]; then
        echo "Killing process on port ${port}: ${pid}"
        kill ${pid} 2>/dev/null || true
    fi
done

sleep 1

# Start Coordinator
echo ""
echo "Starting Coordinator on port ${COORDINATOR_PORT}..."
java -jar ${JAR_FILE} --coordinator --coordinator-port ${COORDINATOR_PORT} \
    > ${BASE_DIR}/coordinator.log 2>&1 &
COORDINATOR_PID=$!
echo "Coordinator started: PID ${COORDINATOR_PID}"

sleep 2

# All nodes in the cluster
ALL_NODES="1:localhost:${NODE1_PORT},2:localhost:${NODE2_PORT},3:localhost:${NODE3_PORT},4:localhost:${NODE4_PORT}"

# Start Node 1 (Leader)
echo ""
echo "Starting Node 1 on port ${NODE1_PORT}..."
java -jar ${JAR_FILE} \
    --address localhost:${NODE1_PORT} \
    --peer-addresses "${ALL_NODES}" \
    --cluster-name ${CLUSTER_NAME} \
    --coordinator localhost:${COORDINATOR_PORT} \
    --data-dir ${BASE_DIR}/node1 \
    > ${BASE_DIR}/node1.log 2>&1 &
NODE1_PID=$!
echo "Node 1 started: PID ${NODE1_PID}"

# Start Node 2
echo ""
echo "Starting Node 2 on port ${NODE2_PORT}..."
java -jar ${JAR_FILE} \
    --address localhost:${NODE2_PORT} \
    --peer-addresses "${ALL_NODES}" \
    --cluster-name ${CLUSTER_NAME} \
    --coordinator localhost:${COORDINATOR_PORT} \
    --data-dir ${BASE_DIR}/node2 \
    > ${BASE_DIR}/node2.log 2>&1 &
NODE2_PID=$!
echo "Node 2 started: PID ${NODE2_PID}"

# Start Node 3
echo ""
echo "Starting Node 3 on port ${NODE3_PORT}..."
java -jar ${JAR_FILE} \
    --address localhost:${NODE3_PORT} \
    --peer-addresses "${ALL_NODES}" \
    --cluster-name ${CLUSTER_NAME} \
    --coordinator localhost:${COORDINATOR_PORT} \
    --data-dir ${BASE_DIR}/node3 \
    > ${BASE_DIR}/node3.log 2>&1 &
NODE3_PID=$!
echo "Node 3 started: PID ${NODE3_PID}"

# Start Node 4
echo ""
echo "Starting Node 4 on port ${NODE4_PORT}..."
java -jar ${JAR_FILE} \
    --address localhost:${NODE4_PORT} \
    --peer-addresses "${ALL_NODES}" \
    --cluster-name ${CLUSTER_NAME} \
    --coordinator localhost:${COORDINATOR_PORT} \
    --data-dir ${BASE_DIR}/node4 \
    > ${BASE_DIR}/node4.log 2>&1 &
NODE4_PID=$!
echo "Node 4 started: PID ${NODE4_PID}"

# Wait for cluster to stabilize
echo ""
echo "Waiting for leader election..."
sleep 5

# Check status
echo ""
echo "Cluster Status:"
echo "==============="
echo "Coordinator: localhost:${COORDINATOR_PORT} (PID ${COORDINATOR_PID})"
echo "Node 1:      localhost:${NODE1_PORT} (PID ${NODE1_PID})"
echo "Node 2:      localhost:${NODE2_PORT} (PID ${NODE2_PID})"
echo "Node 3:      localhost:${NODE3_PORT} (PID ${NODE3_PID})"
echo "Node 4:      localhost:${NODE4_PORT} (PID ${NODE4_PID})"
echo ""
echo "Data dir:    ${BASE_DIR}"
echo ""
echo "Log files:"
echo "  ${BASE_DIR}/coordinator.log"
echo "  ${BASE_DIR}/node1.log"
echo "  ${BASE_DIR}/node2.log"
echo "  ${BASE_DIR}/node3.log"
echo "  ${BASE_DIR}/node4.log"
echo ""
echo "To stop the cluster:"
echo "  kill ${COORDINATOR_PID} ${NODE1_PID} ${NODE2_PID} ${NODE3_PID} ${NODE4_PID}"
echo ""
echo "To run the client:"
echo "  java -jar ${JAR_FILE} --client --coordinator localhost:${COORDINATOR_PORT} --cluster-name ${CLUSTER_NAME}"
