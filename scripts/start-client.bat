@echo off
REM
REM Start a TinyKV client in interactive mode
REM
REM Usage: start-client.bat [cluster-name] [coordinator-port]
REM

setlocal enabledelayedexpansion

set CLUSTER_NAME=%~1
if "%CLUSTER_NAME%"=="" set CLUSTER_NAME=test-cluster

set COORDINATOR_PORT=%~2
if "%COORDINATOR_PORT%"=="" set COORDINATOR_PORT=8000

set JAR_FILE=target\tinykv-1.0-SNAPSHOT.jar

echo TinyKV Client
echo ================
echo Coordinator: localhost:%COORDINATOR_PORT%
echo Cluster:     %CLUSTER_NAME%
echo.

REM Build the JAR if not exists
if not exist "%JAR_FILE%" (
    echo Building JAR...
    call mvn package -DskipTests -q
)

echo Starting interactive client...
echo Type 'help' for available commands, 'exit' to quit.
echo.

java -jar %JAR_FILE% --client --coordinator localhost:%COORDINATOR_PORT% --cluster-name %CLUSTER_NAME%

endlocal
