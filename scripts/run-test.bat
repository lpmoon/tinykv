@echo off
REM
REM Automated test script - starts cluster, runs tests, stops cluster
REM
REM Usage: run-test.bat [cluster-name]
REM

setlocal enabledelayedexpansion

set CLUSTER_NAME=%~1
if "%CLUSTER_NAME%"=="" set CLUSTER_NAME=test-cluster

set COORDINATOR_PORT=8000
set JAR_FILE=target\tinykv-1.0-SNAPSHOT.jar

echo TinyKV Automated Test
echo ====================
echo.

REM Start the cluster
echo Step 1: Starting cluster...
call scripts\start-cluster.bat %CLUSTER_NAME%

REM Wait for cluster to be ready
echo.
echo Step 2: Waiting for cluster to be ready...
timeout /t 8 /nobreak >nul

REM Run some test commands
echo.
echo Step 3: Running test commands...

echo put user:1:name Alice > "%TEMP%\tinykv-test-commands.txt"
echo put user:1:age 30 >> "%TEMP%\tinykv-test-commands.txt"
echo put user:2:name Bob >> "%TEMP%\tinykv-test-commands.txt"
echo put user:2:age 25 >> "%TEMP%\tinykv-test-commands.txt"
echo get user:1:name >> "%TEMP%\tinykv-test-commands.txt"
echo get user:1:age >> "%TEMP%\tinykv-test-commands.txt"
echo get user:2:name >> "%TEMP%\tinykv-test-commands.txt"
echo get user:2:age >> "%TEMP%\tinykv-test-commands.txt"
echo info >> "%TEMP%\tinykv-test-commands.txt"
echo exit >> "%TEMP%\tinykv-test-commands.txt"

echo.
echo Executing commands...
java -jar %JAR_FILE% --client --coordinator localhost:%COORDINATOR_PORT% --cluster-name %CLUSTER_NAME% < "%TEMP%\tinykv-test-commands.txt"

REM Cleanup
del "%TEMP%\tinykv-test-commands.txt" 2>nul

echo.
echo Step 4: Stopping cluster...
call scripts\stop-cluster.bat %CLUSTER_NAME%

echo.
echo Test completed!

endlocal
