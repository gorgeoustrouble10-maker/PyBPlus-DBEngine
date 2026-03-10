"""
WAL 主从复制原型：Master 推送 WAL 到 Slave，Slave 实时重放。

English: Master-Slave WAL replication; Master pushes, Slave receives and replays.
Chinese: WAL 主从复制；Master 推送，Slave 接收并重放。
Japanese: WAL マスタースレーブ複製；Master が Push、Slave が受信してリプレイ。
"""

import base64
import logging
import socket
import struct
import threading
import time
from pathlib import Path
from typing import Any, Optional

from bplus_tree.logging import WriteAheadLog

REPL_HEADER_LEN: int = 4
REPL_POLL_INTERVAL: float = 0.1
FAILOVER_NO_HEARTBEAT_SEC: float = 5.0


def _apply_wal_line(
    tables: dict[str, Any],
    table_name: str,
    line: str,
    state: dict[str, Any],
) -> None:
    """
    Apply a single WAL line to tables; maintains tx pending state.
    Compatible with WriteAheadLog.replay semantics.
    """
    line = line.strip()
    if not line or line == "CHECKPOINT":
        return
    parts = line.split(None, 2)
    if not parts:
        return
    first = parts[0].upper()
    pending: dict[int, list[tuple[str, Any, Optional[Any]]]] = state.setdefault("pending", {})
    if first == "TX_BEGIN" and len(parts) >= 2:
        state["current_tx"] = int(parts[1])
        return
    if first == "COMMIT" and len(parts) >= 2:
        tx_id = int(parts[1])
        tbl = tables.get(table_name)
        if tbl is not None:
            for op, k, v in pending.get(tx_id, []):
                try:
                    if op == "INSERT" and v is not None:
                        tbl._tree.insert(k, v)
                    elif op == "DELETE":
                        tbl._tree.delete(k)
                except Exception:
                    pass
        pending.pop(tx_id, None)
        return
    if first.startswith("TX") and first[2:].isdigit():
        tx_id = int(first[2:])
        rest = (parts[1] + " " + parts[2]) if len(parts) >= 3 else (parts[1] if len(parts) >= 2 else "")
        sub = rest.split(None, 2)
        if len(sub) < 2:
            return
        op = sub[0].upper()
        key_s = sub[1]
        key = int(key_s) if key_s.lstrip("-").isdigit() else key_s
        if op == "INSERT" and len(sub) >= 3:
            val: Any = sub[2]
            try:
                val = base64.b64decode(val.encode("ascii"))
            except Exception:
                pass
            if tx_id not in pending:
                pending[tx_id] = []
            pending[tx_id].append(("INSERT", key, val))
        elif op == "DELETE":
            if tx_id not in pending:
                pending[tx_id] = []
            pending[tx_id].append(("DELETE", key, None))
        return
    op = first
    if len(parts) < 2:
        return
    tbl = tables.get(table_name)
    if tbl is None:
        return
    key_s = parts[1]
    key = int(key_s) if key_s.lstrip("-").isdigit() else key_s
    if op == "INSERT" and len(parts) >= 3:
        insert_val: Any = parts[2]
        try:
            insert_val = base64.b64decode(insert_val.encode("ascii"))
        except Exception:
            pass
        try:
            tbl._tree.insert(key, insert_val)
        except Exception:
            pass
    elif op == "DELETE":
        try:
            tbl._tree.delete(key)
        except Exception:
            pass


class ReplicationPublisher:
    """
    Master-side: tail WAL files and push new lines to connected slaves.
    """

    def __init__(
        self,
        data_dir: Path,
        tables: dict[str, Any],
        replication_port: int,
    ) -> None:
        self._data_dir = Path(data_dir)
        self._tables = tables
        self._replication_port = replication_port
        self._slaves: list[socket.socket] = []
        self._slaves_lock = threading.Lock()
        self._positions: dict[str, int] = {}
        self._stop = threading.Event()
        self._listener: Optional[socket.socket] = None

    def _get_wal_paths(self) -> list[tuple[str, Path]]:
        result: list[tuple[str, Path]] = []
        for name in self._tables:
            p = self._data_dir / f"wal_{name}.log"
            if p.exists():
                result.append((name, p))
                if p.name not in self._positions:
                    self._positions[p.name] = p.stat().st_size
        return result

    def _read_new_lines(self, table_name: str, path: Path) -> list[str]:
        pos = self._positions.get(path.name, 0)
        try:
            with open(path, "r", encoding="utf-8") as f:
                f.seek(pos)
                lines = f.readlines()
                new_pos = f.tell()
                self._positions[path.name] = new_pos
                return lines
        except Exception:
            return []

    def _run_acceptor(self) -> None:
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self._listener.bind(("0.0.0.0", self._replication_port))
            self._listener.listen(5)
            self._listener.settimeout(1.0)
        except Exception as e:
            logging.warning("Replication acceptor bind failed: %s", e)
            return
        while not self._stop.is_set():
            try:
                conn, _ = self._listener.accept()
                conn.settimeout(10.0)
                with self._slaves_lock:
                    self._slaves.append(conn)
                logging.info("Replication slave connected")
            except socket.timeout:
                continue
            except Exception:
                if not self._stop.is_set():
                    logging.warning("Replication accept error")
                break

    def _broadcast(self, table_name: str, line: str) -> None:
        msg = f"{table_name}\t{line}".encode("utf-8")
        payload = struct.pack("<I", len(msg)) + msg
        dead: list[socket.socket] = []
        with self._slaves_lock:
            for s in self._slaves:
                try:
                    s.sendall(payload)
                except Exception:
                    dead.append(s)
            for s in dead:
                self._slaves.remove(s)

    def _run_tailer(self) -> None:
        while not self._stop.is_set():
            for table_name, path in self._get_wal_paths():
                for line in self._read_new_lines(table_name, path):
                    if line.strip():
                        self._broadcast(table_name, line)
            time.sleep(REPL_POLL_INTERVAL)

    def start(self) -> None:
        """Start acceptor and tailer threads."""
        t_acceptor = threading.Thread(target=self._run_acceptor, daemon=True)
        t_acceptor.start()
        t_tailer = threading.Thread(target=self._run_tailer, daemon=True)
        t_tailer.start()

    def stop(self) -> None:
        """Stop and cleanup."""
        self._stop.set()
        with self._slaves_lock:
            for s in self._slaves:
                try:
                    s.close()
                except Exception:
                    pass
            self._slaves.clear()
        if self._listener:
            try:
                self._listener.close()
            except Exception:
                pass


class ReplicationSubscriber:
    """
    Slave-side: connect to Master, receive WAL stream, replay into local tables.
    Phase 25: Health check; if no WAL for FAILOVER_NO_HEARTBEAT_SEC, promote to Master.
    """

    def __init__(
        self,
        master_addr: str,
        master_port: int,
        tables: dict[str, Any],
        replication_info_ref: Optional[dict[str, Any]] = None,
        failover_timeout_sec: float = FAILOVER_NO_HEARTBEAT_SEC,
    ) -> None:
        self._master_addr = master_addr
        self._master_port = master_port
        self._tables = tables
        self._replication_info_ref = replication_info_ref or {}
        self._failover_timeout = failover_timeout_sec
        self._state: dict[str, Any] = {}
        self._stop = threading.Event()
        self._last_receive_time: float = 0.0
        self._promoted = threading.Event()

    @property
    def replication_lag(self) -> float:
        """Seconds since last received WAL line."""
        if self._last_receive_time <= 0:
            return float("nan")
        return time.monotonic() - self._last_receive_time

    def promote_to_master(self) -> None:
        """
        Stop subscription, switch to MASTER role; promoted instance accepts writes and logs WAL.
        """
        if self._promoted.is_set():
            return
        self._promoted.set()
        self._stop.set()
        self._replication_info_ref["node_role"] = "MASTER"
        self._replication_info_ref["replication_lag"] = "N/A"
        if "get_lag" in self._replication_info_ref:
            del self._replication_info_ref["get_lag"]
        logging.info("Promoted to MASTER (failover)")

    def _run_health_check(self) -> None:
        """If no WAL/heartbeat for failover_timeout, trigger promote."""
        while not self._stop.is_set():
            time.sleep(0.2)
            if self._stop.is_set():
                break
            if self._last_receive_time <= 0:
                continue
            if time.monotonic() - self._last_receive_time >= self._failover_timeout:
                self.promote_to_master()
                break

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5.0)
                sock.connect((self._master_addr, self._master_port))
                logging.info("Replication connected to master %s:%d", self._master_addr, self._master_port)
                while not self._stop.is_set():
                    header = sock.recv(REPL_HEADER_LEN)
                    if len(header) < REPL_HEADER_LEN:
                        break
                    length = struct.unpack("<I", header)[0]
                    if length > 10 * 1024 * 1024:
                        break
                    data = b""
                    while len(data) < length:
                        chunk = sock.recv(length - len(data))
                        if not chunk:
                            break
                        data += chunk
                    if len(data) < length:
                        break
                    msg = data.decode("utf-8", errors="replace")
                    if "\t" in msg:
                        table_name, line = msg.split("\t", 1)
                        _apply_wal_line(self._tables, table_name, line, self._state)
                        self._last_receive_time = time.monotonic()
                        if self._replication_info_ref is not None:
                            self._replication_info_ref["replication_lag"] = f"{self.replication_lag:.3f}s"
            except Exception as e:
                if not self._stop.is_set():
                    logging.warning("Replication subscriber error: %s", e)
            time.sleep(1.0)

    def start(self) -> None:
        """Start subscription and health-check threads."""
        t = threading.Thread(target=self._run, daemon=True)
        t.start()
        t_hc = threading.Thread(target=self._run_health_check, daemon=True)
        t_hc.start()

    def stop(self) -> None:
        """Stop subscription."""
        self._stop.set()
