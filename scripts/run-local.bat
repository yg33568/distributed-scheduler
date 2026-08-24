@echo off
REM =====================================================================
REM 本地手动启动（无 Docker 时兜底）
REM 前置条件：
REM   1. 本机已启动 ZooKeeper(2181)、MySQL(3306)、Redis(6379)
REM   2. 已在 MySQL 执行 docker/initdb/01-schema.sql 建库建表
REM   3. 已执行 mvn package 生成 fat-jar
REM 用法：先 mvn package，再运行本脚本
REM =====================================================================
setlocal

set DB_JDBC_URL=jdbc:mysql://localhost:3306/scheduler?useSSL=false&serverTimezone=UTC
set DB_USERNAME=root
set DB_PASSWORD=123456
set ZK_CONNECT=localhost:2181
set REDIS_HOST=localhost
set REDIS_PORT=6379

REM 调度中心（master，通过 ZK 参与选主）
echo [1/3] 启动 scheduler-a (TCP 8080 / admin 8081)
set SCHEDULER_PORT=8080
set SCHEDULER_ADMIN_PORT=8081
set SCHEDULER_ADVERTISE_HOST=localhost
start "scheduler-a" java -jar scheduler-core\target\scheduler-core-1.0-SNAPSHOT-all.jar

REM 第二个 master（主备，如需演示故障切换就开，admin 用 9081）
echo [2/3] 启动 scheduler-b (TCP 8090 / admin 9081，可选)
set SCHEDULER_PORT=8090
set SCHEDULER_ADMIN_PORT=9081
set SCHEDULER_ADVERTISE_HOST=localhost
start "scheduler-b" java -jar scheduler-core\target\scheduler-core-1.0-SNAPSHOT-all.jar

REM worker
echo [3/3] 启动 worker-a
set WORKER_ID=worker-a
set WORKER_ADVERTISE_HOST=localhost
start "worker-a" java -jar scheduler-worker\target\scheduler-worker-1.0-SNAPSHOT-all.jar

echo.
echo 查看状态：curl http://localhost:8081/status
echo 演示故障切换：关掉 scheduler-a 进程，观察 scheduler-b 接管
endlocal
