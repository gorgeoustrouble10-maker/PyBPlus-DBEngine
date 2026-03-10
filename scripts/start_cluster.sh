#!/bin/bash
# PyBPlus-DBEngine 一主一从集群启动脚本
# One Master + One Slave cluster launcher

set -e
cd "$(dirname "$0")/.."
DATA_MASTER="./data_master"
DATA_SLAVE="./data_slave"
PORT_MASTER=8765
PORT_SLAVE=8766
PORT_REPL=8767

rm -rf "$DATA_MASTER" "$DATA_SLAVE"
mkdir -p "$DATA_MASTER" "$DATA_SLAVE"

echo "Starting Master on port $PORT_MASTER (replication on $PORT_REPL)..."
python scripts/run_server.py -d "$DATA_MASTER" -H 127.0.0.1 -P $PORT_MASTER --replication-port $PORT_REPL &
MASTER_PID=$!
sleep 2

echo "Creating table on Master..."
(echo "CREATE TABLE stress (id INT, v VARCHAR(32));"; echo "QUIT") | python scripts/cli_client.py -H 127.0.0.1 -P $PORT_MASTER 2>/dev/null || true
sleep 1

echo "Starting Slave on port $PORT_SLAVE (--slave-of 127.0.0.1:$PORT_REPL)..."
python scripts/run_server.py -d "$DATA_SLAVE" -H 127.0.0.1 -P $PORT_SLAVE --slave-of "127.0.0.1:$PORT_REPL" &
SLAVE_PID=$!
sleep 2

echo "Creating table on Slave (same schema as Master)..."
(echo "CREATE TABLE stress (id INT, v VARCHAR(32));"; echo "QUIT") | python scripts/cli_client.py -H 127.0.0.1 -P $PORT_SLAVE 2>/dev/null || true
sleep 1

echo ""
echo "Cluster ready. Master: localhost:$PORT_MASTER, Slave: localhost:$PORT_SLAVE"
echo "Run: echo 'INSERT INTO stress VALUES (1, \"x\");' | python scripts/cli_client.py -P $PORT_MASTER"
echo "Run: echo 'SELECT * FROM stress;' | python scripts/cli_client.py -P $PORT_SLAVE"
echo "Press Ctrl+C to stop."
wait $MASTER_PID $SLAVE_PID 2>/dev/null || true
