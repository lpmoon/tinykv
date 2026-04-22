@echo off
REM
REM Stop the TinyKV cluster
REM
REM Usage: stop-cluster.bat [cluster-name]
REM

setlocal enabledelayedexpansion

set CLUSTER_NAME=%~1
if "%CLUSTER_NAME%"=="" set CLUSTER_NAME=test-cluster

set COORDINATOR_PORT=8000
set NODE1_PORT=7000
set NODE2_PORT=7001
set NODE3_PORT=7002
set NODE4_PORT=7003

echo Stopping TinyKV Cluster: %CLUSTER_NAME%

REM Kill processes on all ports
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :%COORDINATOR_PORT%') do (
    echo Stopping process on port %COORDINATOR_PORT%: %%a
    taskkill /F /PID %%a 2>nul
)

for /f "tokens=5" %%a in ('netstat -ano ^| findstr :%NODE1_PORT%') do (
    echo Stopping process on port %NODE1_PORT%: %%a
    taskkill /F /PID %%a 2>nul
)

for /f "tokens=5" %%a in ('netstat -ano ^| findstr :%NODE2_PORT%') do (
    echo Stopping process on port %NODE2_PORT%: %%a
    taskkill /F /PID %%a 2>nul
)

for /f "tokens=5" %%a in ('netstat -ano ^| findstr :%NODE3_PORT%') do (
    echo Stopping process on port %NODE3_PORT%: %%a
    taskkill /F /PID %%a 2>nul
)

for /f "tokens=5" %%a in ('netstat -ano ^| findstr :%NODE4_PORT%') do (
    echo Stopping process on port %NODE4_PORT%: %%a
    taskkill /F /PID %%a 2>nul
)

echo Done.

endlocal
