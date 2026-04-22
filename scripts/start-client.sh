#!/bin/bash
#
# Start a TinyKV client and run test commands
#
# Usage: ./scripts/start-client.sh [cluster-name]
#

set -e

CLUSTER_NAME=${1:-test-cluster}
COORDINATOR_PORT=${2:-8000}
COORDINATOR="localhost:${COORDINATOR_PORT}"

JAR_FILE="target/tinykv-1.0-SNAPSHOT.jar"

echo "TinyKV Client Test"
echo "=================="
echo "Coordinator: ${COORDINATOR}"
echo "Cluster:     ${CLUSTER_NAME}"
echo ""

# Build the JAR if not exists
if [ ! -f "${JAR_FILE}" ]; then
    echo "Building JAR..."
    mvn package -DskipTests -q
fi

# Run the client in interactive mode
echo "Starting interactive client..."
echo "Type 'help' for available commands, 'exit' to quit."
echo ""

java -jar ${JAR_FILE} --client --coordinator ${COORDINATOR} --cluster-name ${CLUSTER_NAME}
