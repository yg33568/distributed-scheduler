#!/usr/bin/env bash
# =====================================================================
# 本地手动启动（无 Docker 时兜底，bash/WSL 环境）
# 前置条件同 run-local.bat：ZK(2181) / MySQL(3306) / Redis(6379) + 已建表 + 已 mvn package
# =====================================================================
set -e

export DB_JDBC_URL="jdbc:mysql://localhost:3306/scheduler?useSSL=false&serverTimezone=UTC"
export DB_USERNAME="root"
export DB_PASSWORD="123456"
export ZK_CONNECT="localhost:2181"
export REDIS_HOST="localhost"
export REDIS_PORT="6379"

echo "[1/3] 启动 scheduler-a (TCP 8080 / admin 8081)"
export SCHEDULER_PORT=8080 SCHEDULER_ADMIN_PORT=8081 SCHEDULER_ADVERTISE_HOST=localhost
java -jar scheduler-core/target/scheduler-core-1.0-SNAPSHOT-all.jar &
MASTER_A_PID=$!

echo "[2/3] 启动 scheduler-b (TCP 8090 / admin 9081，可选)"
export SCHEDULER_PORT=8090 SCHEDULER_ADMIN_PORT=9081 SCHEDULER_ADVERTISE_HOST=localhost
java -jar scheduler-core/target/scheduler-core-1.0-SNAPSHOT-all.jar &
MASTER_B_PID=$!

echo "[3/3] 启动 worker-a"
export WORKER_ID=worker-a WORKER_ADVERTISE_HOST=localhost
java -jar scheduler-worker/target/scheduler-worker-1.0-SNAPSHOT-all.jar &
WORKER_PID=$!

echo
echo "查看状态: curl http://localhost:8081/status"
echo "停止: kill $MASTER_A_PID $MASTER_B_PID $WORKER_PID"
