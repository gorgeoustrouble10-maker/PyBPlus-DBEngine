#!/usr/bin/env python3
"""
并发压力测试：20 客户端持续 30 秒高频 INSERT 与 SELECT COUNT(*)。

English: Concurrency stress test; 20 clients, 30 seconds, high-frequency INSERT + SELECT COUNT(*).
Chinese: 并发压力测试；20 客户端、30 秒、高频 INSERT 与 SELECT COUNT(*)。
Japanese: 並行ストレステスト；20 クライアント、30 秒、高頻度 INSERT と SELECT COUNT(*)。
"""

import argparse
import logging
import random
import struct
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

MSG_HEADER_LEN = 4
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
NUM_WORKERS = 20
DURATION_SEC = 30


def send_query(sock: object, sql: str) -> tuple[str, str]:
    """
    English: Send SQL, return (status_line, full_response).
    Chinese: 发送 SQL，返回 (状态行, 完整响应)。
    Japanese: SQL を送信、(status_line, full_response) を返す。
    """
    payload = sql.encode("utf-8")
    sock.sendall(struct.pack("<I", len(payload)) + payload)
    header = sock.recv(MSG_HEADER_LEN)
    if len(header) < MSG_HEADER_LEN:
        return ("", "")
    length = struct.unpack("<I", header)[0]
    if length > 10 * 1024 * 1024:
        return ("ERROR", "Response too large")
    data = b""
    while len(data) < length:
        chunk = sock.recv(length - len(data))
        if not chunk:
            break
        data += chunk
    resp = data.decode("utf-8", errors="replace")
    parts = resp.split("\n", 2)
    status = parts[0] if parts else ""
    return (status, resp)


def worker(
    worker_id: int,
    host: str,
    port: int,
    counter_lock: threading.Lock,
    next_id: list[int],
    inserts_done: list[int],
    selects_done: list[int],
    errors: list[str],
    stop_event: threading.Event,
) -> None:
    """
    English: Worker loop; random INSERT or SELECT COUNT(*), run until stop.
    Chinese: Worker 循环；随机 INSERT 或 SELECT COUNT(*)，直至停止。
    Japanese: Worker ループ；ランダムに INSERT または SELECT COUNT(*)、停止まで実行。
    """
    import socket

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10.0)
        sock.connect((host, port))
    except Exception as e:
        with counter_lock:
            errors.append(f"Worker {worker_id} connect: {e}")
        return

    inserts = 0
    selects = 0
    while not stop_event.is_set():
        try:
            if random.random() < 0.5:
                with counter_lock:
                    key = next_id[0]
                    next_id[0] += 1
                sql = f"INSERT INTO stress (id, v) VALUES ({key}, 'w{worker_id}')"
                status, _ = send_query(sock, sql)
                if status == "OK":
                    inserts += 1
                elif status == "ERROR":
                    pass
            else:
                status, resp = send_query(sock, "SELECT COUNT(*) FROM stress")
                if status == "OK":
                    selects += 1
        except Exception as e:
            with counter_lock:
                errors.append(f"Worker {worker_id}: {e}")
            break

    sock.close()
    with counter_lock:
        inserts_done[0] += inserts
        selects_done[0] += selects


def run_benchmark(
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    workers: int = NUM_WORKERS,
    duration: float = DURATION_SEC,
) -> None:
    """
    English: Run concurrency benchmark; assume server already running with table 'stress'.
    Chinese: 运行并发基准测试；假定服务器已启动且存在表 stress。
    Japanese: 並行ベンチマークを実行；サーバー稼働中、テーブル stress 存在を前提。
    """
    counter_lock = threading.Lock()
    next_id = [1]
    inserts_done = [0]
    selects_done = [0]
    errors: list[str] = []

    stop_event = threading.Event()
    threads = []
    for i in range(workers):
        t = threading.Thread(
            target=worker,
            args=(
                i,
                host,
                port,
                counter_lock,
                next_id,
                inserts_done,
                selects_done,
                errors,
                stop_event,
            ),
        )
        threads.append(t)
        t.start()

    time.sleep(duration)
    stop_event.set()
    for t in threads:
        t.join(timeout=5.0)

    logging.info("Inserts: %d", inserts_done[0])
    logging.info("Selects: %d", selects_done[0])
    if errors:
        logging.error("Errors: %d", len(errors))
        for e in errors[:10]:
            logging.error("  - %s", e)
    else:
        logging.info("No deadlocks or exceptions.")


def setup_table(host: str, port: int) -> bool:
    """
    English: Create table 'stress' and ensure it exists.
    Chinese: 创建表 stress 并确认存在。
    Japanese: テーブル stress を作成し存在を確認。
    """
    import socket

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        sock.connect((host, port))
    except Exception as e:
        logging.error("Connect failed: %s", e)
        return False

    for sql in ["CREATE TABLE stress (id INT, v VARCHAR(32))", "SHOW TABLES"]:
        status, resp = send_query(sock, sql)
        if status == "ERROR":
            if "already exists" in resp:
                break
            logging.error("Setup error: %s", resp)
            sock.close()
            return False
    sock.close()
    return True


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description="PyBPlus-DB concurrency stress test")
    parser.add_argument("-H", "--host", default=DEFAULT_HOST, help="Server host")
    parser.add_argument("-P", "--port", type=int, default=DEFAULT_PORT, help="Server port")
    parser.add_argument("-w", "--workers", type=int, default=NUM_WORKERS, help="Number of workers")
    parser.add_argument("-d", "--duration", type=float, default=DURATION_SEC, help="Duration in seconds")
    parser.add_argument("--no-setup", action="store_true", help="Skip CREATE TABLE (table must exist)")
    args = parser.parse_args()

    if not args.no_setup:
        logging.info("Setting up table...")
        if not setup_table(args.host, args.port):
            sys.exit(1)
        logging.info("Table ready.")

    logging.info("Starting %d workers for %.1fs...", args.workers, args.duration)
    run_benchmark(
        host=args.host,
        port=args.port,
        workers=args.workers,
        duration=args.duration,
    )
    logging.info("Done.")


if __name__ == "__main__":
    main()
