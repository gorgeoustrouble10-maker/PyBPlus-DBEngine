#!/usr/bin/env python3
"""
启动 PyBPlus-DB 多线程 TCP 服务器。

English: Start PyBPlus-DB TCP server; -d for data_dir enables recovery + CREATE TABLE.
Chinese: 启动 PyBPlus-DB TCP 服务器；-d 指定数据目录启用恢复与 CREATE TABLE。
Japanese: PyBPlus-DB TCP サーバーを起動；-d でデータディレクトリ指定、リカバリと CREATE TABLE 有効。
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from bplus_tree.schema import Schema
from bplus_tree.table import RowTable
from bplus_tree.server import run_server, run_server_with_recovery


def main() -> None:
    parser = argparse.ArgumentParser(description="PyBPlus-DB Server")
    parser.add_argument("-H", "--host", default="127.0.0.1", help="Bind host")
    parser.add_argument("-P", "--port", type=int, default=8765, help="Bind port")
    parser.add_argument("-d", "--data-dir", default=None, help="Data directory; enables catalog + WAL recovery")
    parser.add_argument("--password", default=None, help="Require AUTH <password> as first message")
    parser.add_argument("--replication-port", type=int, default=None, help="Master: listen for replication on this port (e.g. 8767)")
    parser.add_argument("--slave-of", default=None, help="Slave: replicate from master (e.g. 127.0.0.1:8767)")
    args = parser.parse_args()

    if args.data_dir:
        run_server_with_recovery(
            args.data_dir,
            host=args.host,
            port=args.port,
            password=args.password,
            replication_port=args.replication_port,
            slave_of=args.slave_of,
        )
    else:
        schema = Schema(fields=[("id", "INT"), ("name", "VARCHAR(32)"), ("score", "FLOAT")])
        table = RowTable(schema, primary_key="id")
        run_server(table=table, host=args.host, port=args.port, password=args.password)


if __name__ == "__main__":
    main()
