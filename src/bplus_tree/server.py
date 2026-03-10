"""
多线程 TCP 数据库服务器；Wire Protocol [Length][Payload]；连接超时与清理。

English: Multi-threaded TCP database server; Wire Protocol [Length][Payload]; timeout & cleanup.
Chinese: 多线程 TCP 数据库服务器；Wire Protocol；连接超时与资源清理。
Japanese: マルチスレッド TCP データベースサーバー；Wire Protocol；接続タイムアウトとリソース解放。
"""

import logging
import socket
import struct
import socketserver
from pathlib import Path
from typing import Any, Optional

# 客户端空闲超时（秒）；超时后强制回滚未提交事务并断开
CLIENT_IDLE_TIMEOUT: float = 60.0

from bplus_tree.errors import DBError
from bplus_tree.table import RowTable
from bplus_tree.transaction import TransactionManager
from bplus_tree.sql_engine import execute_sql, parse_sql

# Wire Protocol: [4 bytes length LE][payload UTF-8]
MSG_HEADER_LEN: int = 4


def _encode_response_correct(
    status: str,
    message: str,
    rows: list[list[Any]],
    columns: Optional[list[str]] = None,
) -> bytes:
    """Encode response: STATUS\\nmessage\\n[header_row\\n]data_rows."""
    lines = [status, message]
    if rows:
        if columns:
            lines.append("\t".join(columns))
        for r in rows:
            lines.append("\t".join(str(v) for v in r))
    return "\n".join(lines).encode("utf-8")


class DBRequestHandler(socketserver.BaseRequestHandler):
    """
    English: Handle one client connection; maintains per-connection Transaction.
    Chinese: 处理单客户端连接；每连接维护独立 Transaction。
    Japanese: 1 クライアント接続を処理；接続ごとに Transaction を保持。
    """

    def setup(self) -> None:
        """
        English: Set socket timeout; 60s idle forces rollback and disconnect.
        Chinese: 设置 socket 超时；60 秒无响应则强制回滚并断开。
        Japanese: ソケットタイムアウトを設定；60秒無応答で強制ロールバックと切断。
        """
        self._tx: Optional[Any] = None
        self._tx_manager = getattr(self.server, "tx_manager", None)
        self._table = getattr(self.server, "table", None)
        self._db = getattr(self.server, "db", None)
        self._authenticated = getattr(self.server, "password", None) is None
        if hasattr(self.request, "settimeout"):
            self.request.settimeout(CLIENT_IDLE_TIMEOUT)

    def handle(self) -> None:
        db = self._db
        table = self._table
        if db is None and table is None:
            self._send_error("[1049] No database or table configured")
            return
        tx_manager = self._tx_manager
        password = getattr(self.server, "password", None)
        while True:
            try:
                raw = self._read_message()
                if raw is None:
                    break
                sql = raw.decode("utf-8").strip()
                if not self._authenticated and password is not None:
                    if sql.upper().startswith("AUTH "):
                        if sql[5:].strip() == password:
                            self._authenticated = True
                            self._send_ok("Authenticated")
                        else:
                            self._send_error("[1045] Access denied")
                            break
                    else:
                        self._send_error("[1045] Authentication required; send AUTH <password> first")
                        break
                    continue
                if not sql:
                    continue
                if sql.upper() in ("QUIT", "EXIT", "BYE"):
                    break
                if sql.upper().startswith("BEGIN"):
                    if tx_manager and self._tx is None:
                        self._tx = tx_manager.begin()
                        self._send_ok("Transaction started")
                    continue
                if sql.upper().startswith("COMMIT"):
                    if self._tx and tx_manager:
                        tx_manager.commit(self._tx)
                        self._tx = None
                        self._send_ok("Committed")
                    continue
                if sql.upper().startswith("ROLLBACK TO"):
                    if self._tx and tx_manager:
                        try:
                            parsed = parse_sql(sql)
                            if hasattr(parsed, "name"):
                                self._tx.rollback_to(parsed.name)
                                self._send_ok(f"Rolled back to {parsed.name}")
                            else:
                                self._tx.rollback()
                                tx_manager.abort(self._tx)
                                self._tx = None
                                self._send_ok("Rolled back")
                        except (ValueError, Exception) as e:
                            self._send_error(f"[1064] {e}")
                    continue
                if sql.upper().startswith("ROLLBACK"):
                    if self._tx and tx_manager:
                        self._tx.rollback()
                        tx_manager.abort(self._tx)
                        self._tx = None
                        self._send_ok("Rolled back")
                    continue
                if sql.upper().startswith("SAVEPOINT"):
                    if self._tx and tx_manager:
                        try:
                            parsed = parse_sql(sql)
                            if hasattr(parsed, "name"):
                                self._tx.savepoint(parsed.name)
                                self._send_ok(f"Savepoint {parsed.name} created")
                            else:
                                self._send_error("[1064] Invalid SAVEPOINT")
                        except (ValueError, Exception) as e:
                            self._send_error(f"[1064] {e}")
                    else:
                        self._send_error("[1064] No active transaction for SAVEPOINT")
                    continue

                if db is not None:
                    msg, rows, columns = execute_sql(
                        sql, db=db, tx=self._tx, tx_manager=tx_manager
                    )
                else:
                    msg, rows, columns = execute_sql(sql, table=table, tx=self._tx)
                payload = _encode_response_correct("OK", msg, rows, columns)
                self._send_raw(payload)
            except socket.timeout:
                if self._tx and tx_manager:
                    try:
                        self._tx.rollback()
                        tx_manager.abort(self._tx)
                    except Exception:
                        pass
                    self._tx = None
                break
            except DBError as e:
                self._send_error(e.format_for_wire())
            except Exception as e:
                self._send_error(f"[1050] {e}")

    def _read_message(self) -> Optional[bytes]:
        """Read [Length][Payload] from socket."""
        try:
            header = self.request.recv(MSG_HEADER_LEN)
            if len(header) < MSG_HEADER_LEN:
                return None
            length = struct.unpack("<I", header)[0]
            if length > 1024 * 1024:
                return None
            data = b""
            while len(data) < length:
                chunk = self.request.recv(length - len(data))
                if not chunk:
                    return None
                data += chunk
            return data
        except (ConnectionResetError, BrokenPipeError, OSError):
            return None

    def _send_raw(self, payload: bytes) -> None:
        try:
            self.request.sendall(struct.pack("<I", len(payload)) + payload)
        except (BrokenPipeError, OSError):
            pass

    def _send_ok(self, msg: str) -> None:
        self._send_raw(_encode_response_correct("OK", msg, []))

    def _send_error(self, err: str) -> None:
        self._send_raw(_encode_response_correct("ERROR", err, []))


class DBServer(socketserver.ThreadingTCPServer):
    """
    English: TCP server with table/db, tx_manager, password attributes.
    Chinese: 带 table/db、tx_manager、password 属性的 TCP 服务器。
    Japanese: table/db、tx_manager、password 属性を持つ TCP サーバー。
    """

    table: Optional[RowTable] = None
    tx_manager: Optional[Any] = None
    db: Optional[Any] = None
    password: Optional[str] = None


def run_server(
    table: Optional[RowTable] = None,
    db: Optional[Any] = None,
    host: str = "127.0.0.1",
    port: int = 8765,
    password: Optional[str] = None,
) -> None:
    """
    English: Start multi-threaded TCP server; use db (with recovery) or table.
    Chinese: 启动多线程 TCP 服务器；使用 db（含恢复）或单表。
    Japanese: マルチスレッド TCP サーバーを起動；db（リカバリ付き）または table。
    """
    if table is None and db is None:
        raise ValueError("Either table or db must be provided")
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    with DBServer((host, port), DBRequestHandler) as server:
        server.table = table
        server.db = db
        server.tx_manager = TransactionManager()
        server.password = password
        server.daemon_threads = True
        server.allow_reuse_address = True
        logging.info("PyBPlus-DB Server on %s:%d", host, port)
        server.serve_forever()


def run_server_with_recovery(
    data_dir: str | Path,
    host: str = "127.0.0.1",
    port: int = 8765,
    password: str | None = None,
) -> None:
    """
    English: Start server with Catalog + WAL recovery; CREATE TABLE supported.
    Chinese: 启动服务器，加载 Catalog 并执行 WAL 恢复；支持 CREATE TABLE。
    Japanese: Catalog と WAL リカバリでサーバーを起動；CREATE TABLE 対応。
    """
    from bplus_tree.database_context import DatabaseContext

    ctx = DatabaseContext(Path(data_dir) if not isinstance(data_dir, Path) else data_dir)
    ctx.load_tables()
    ctx.run_recovery()
    run_server(db=ctx, host=host, port=port, password=password)
