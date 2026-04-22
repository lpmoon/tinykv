@echo off
REM
REM Start a 3-node TinyKV cluster with a Coordinator
REM
REM Usage:
REM   start-cluster.bat [cluster-name]   Start with existing data
REM   start-cluster.bat [cluster-name] --clean   Start fresh (delete data)
REM

setlocal enabledelayedexpansion

set CLEAN_DATA=
set CLUSTER_NAME=

REM Parse arguments
:parse_args
if "%~1"=="" goto done_args
if "%~1"=="--clean" (
    set CLEAN_DATA=1
    shift
    goto parse_args
)
if "%~1"=="--" goto :eof
set CLUSTER_NAME=%~1
shift
goto parse_args

:done_args
if "%CLUSTER_NAME%"=="" set CLUSTER_NAME=test-cluster

set BASE_DIR=%TEMP%\tinykv-%CLUSTER_NAME%
set COORDINATOR_PORT=8000
set NODE1_PORT=7000
set NODE2_PORT=7001
set NODE3_PORT=7002
set NODE4_PORT=7003

set JAR_FILE=target\tinykv-1.0-SNAPSHOT.jar

echo Starting TinyKV Cluster: %CLUSTER_NAME%
echo ==========================================

REM Create data directories
if defined CLEAN_DATA (
    echo WARNING: Cleaning existing data...
    if exist "%BASE_DIR%" rmdir /s /q "%BASE_DIR%"
)
if not exist "%BASE_DIR%\node1" mkdir "%BASE_DIR%\node1"
if not exist "%BASE_DIR%\node2" mkdir "%BASE_DIR%\node2"
if not exist "%BASE_DIR%\node3" mkdir "%BASE_DIR%\node3"
if not exist "%BASE_DIR%\node4" mkdir "%BASE_DIR%\node4"

REM Build the JAR if not exists
if not exist "%JAR_FILE%" (
    echo Building JAR...
    call mvn package -DskipTests -q
)

REM Kill any existing processes on these ports
echo Killing existing processes on ports %COORDINATOR_PORT%, %NODE1_PORT%, %NODE2_PORT%, %NODE3_PORT%, %NODE4_PORT%...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :%COORDINATOR_PORT%') do taskkill /F /PID %%a 2>nul
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :%NODE1_PORT%') do taskkill /F /PID %%a 2>nul
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :%NODE2_PORT%') do taskkill /F /PID %%a 2>nul
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :%NODE3_PORT%') do taskkill /F /PID %%a 2>nul
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :%NODE4_PORT%') do taskkill /F /PID %%a 2>nul

timeout /t 1 /nobreak >nul

REM All nodes in the cluster
set ALL_NODES=1:localhost:%NODE1_PORT%,2:localhost:%NODE2_PORT%,3:localhost:%NODE3_PORT%,4:localhost:%NODE4_PORT%

REM Start Coordinator
echo.
echo Starting Coordinator on port %COORDINATOR_PORT%...
start /B java -jar %JAR_FILE% --coordinator --coordinator-port %COORDINATOR_PORT% > "%BASE_DIR%\coordinator.log" 2>&1

timeout /t 2 /nobreak >nul

REM Start Node 1
echo.
echo Starting Node 1 on port %NODE1_PORT%...
start /B java -jar %JAR_FILE% --address localhost:%NODE1_PORT% --peer-addresses "%ALL_NODES%" --cluster-name %CLUSTER_NAME% --coordinator localhost:%COORDINATOR_PORT% --data-dir "%BASE_DIR%\node1" > "%BASE_DIR%\node1.log" 2>&1

REM Start Node 2
echo.
echo Starting Node 2 on port %NODE2_PORT%...
start /B java -jar %JAR_FILE% --address localhost:%NODE2_PORT% --peer-addresses "%ALL_NODES%" --cluster-name %CLUSTER_NAME% --coordinator localhost:%COORDINATOR_PORT% --data-dir "%BASE_DIR%\node2" > "%BASE_DIR%\node2.log" 2>&1

REM Start Node 3
echo.
echo Starting Node 3 on port %NODE3_PORT%...
start /B java -jar %JAR_FILE% --address localhost:%NODE3_PORT% --peer-addresses "%ALL_NODES%" --cluster-name %CLUSTER_NAME% --coordinator localhost:%COORDINATOR_PORT% --data-dir "%BASE_DIR%\node3" > "%BASE_DIR%\node3.log" 2>&1

REM Start Node 4
echo.
echo Starting Node 4 on port %NODE4_PORT%...
start /B java -jar %JAR_FILE% --address localhost:%NODE4_PORT% --peer-addresses "%ALL_NODES%" --cluster-name %CLUSTER_NAME% --coordinator localhost:%COORDINATOR_PORT% --data-dir "%BASE_DIR%\node4" > "%BASE_DIR%\node4.log" 2>&1

REM Wait for cluster to stabilize
echo.
echo Waiting for leader election...
timeout /t 5 /nobreak >nul

REM Check status
echo.
echo Cluster Status:
echo ===============
echo Coordinator: localhost:%COORDINATOR_PORT%
echo Node 1:      localhost:%NODE1_PORT%
echo Node 2:      localhost:%NODE2_PORT%
echo Node 3:      localhost:%NODE3_PORT%
echo Node 4:      localhost:%NODE4_PORT%
echo.
echo Data dir:    %BASE_DIR%
echo.
echo Console:     http://localhost:8001
echo.
echo To stop the cluster:
echo   scripts\stop-cluster.bat %CLUSTER_NAME%
echo.
echo To run the client:
echo   java -jar %JAR_FILE% --client --coordinator localhost:%COORDINATOR_PORT% --cluster-name %CLUSTER_NAME%

endlocal
