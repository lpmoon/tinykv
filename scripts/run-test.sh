#!/bin/bash
#
# Automated test script - starts cluster, runs tests, stops cluster
#
# Usage: ./scripts/run-test.sh [cluster-name]
#

set -e

CLUSTER_NAME=${1:-test-cluster}
COORDINATOR_PORT=8000
BASE_DIR="/tmp/tinykv-${CLUSTER_NAME}"

JAR_FILE="target/tinykv-1.0-SNAPSHOT.jar"

echo "TinyKV Automated Test"
echo "===================="
echo ""

# Start the cluster
echo "Step 1: Starting cluster..."
bash scripts/start-cluster.sh ${CLUSTER_NAME}

# Wait for cluster to be ready
echo ""
echo "Step 2: Waiting for cluster to be ready..."
sleep 8

# Run some test commands
echo ""
echo "Step 3: Running test commands..."

# Create a temporary script for the client commands
TEMP_SCRIPT="/tmp/tinykv-test-commands.txt"
cat > ${TEMP_SCRIPT} << 'EOF'
info
put user:1:name Alice
put user:1:age 30
put user:2:name Bob
put user:2:age 25
get user:1:name
get user:1:age
get user:2:name
get user:2:age
scan user:1 user:2
delete user:2:age
get user:2:age
info
exit
EOF

echo ""
echo "Executing commands..."
java -jar ${JAR_FILE} --client --coordinator localhost:${COORDINATOR_PORT} --cluster-name ${CLUSTER_NAME} < ${TEMP_SCRIPT}

# Cleanup
rm -f ${TEMP_SCRIPT}

echo ""
echo "Step 4: Stopping cluster..."
bash scripts/stop-cluster.sh ${CLUSTER_NAME}

echo ""
echo "Test completed!"
