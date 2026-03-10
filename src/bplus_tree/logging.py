"""
预写日志 (WAL - Write Ahead Log)，支持事务原子提交。

English: Write-ahead logging for crash recovery; supports atomic transaction commit.
Chinese: 预写日志：崩溃恢复；支持事务原子提交（只有 COMMIT 后操作在重启后生效）。
Japanese: ライトアヘッドログ：クラッシュ復旧；トランザクションのアトミックコミット対応。
"""

import base64
import os
from pathlib import Path
from typing import Any, Generator, Optional


class WriteAheadLog:
    """
    English: Append-only WAL; log INSERT/DELETE before modifying pages.
    Chinese: 追加式预写日志；在修改页面前记录 INSERT/DELETE。
    Japanese: 追記式 WAL；ページ変更前に INSERT/DELETE を記録します。
    """

    def __init__(self, filepath: str | Path) -> None:
        """
        English: Open/create WAL file for appending.
        Chinese: 打开或创建 WAL 文件用于追加写入。
        Japanese: WAL ファイルを開くか作成し、追記用に初期化します。
        """
        self._path = Path(filepath)
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def log_tx_begin(self, tx_id: int) -> None:
        """
        English: Log transaction begin; groups subsequent operations until COMMIT.
        Chinese: 记录事务开始；将后续操作归入该事务，直到 COMMIT。
        Japanese: トランザクション開始を記録；COMMIT までの操作をグルーピング。
        """
        line = f"TX_BEGIN {tx_id}\n"
        with open(self._path, "a", encoding="utf-8") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())

    def log_commit(self, tx_id: int) -> None:
        """
        English: Log transaction commit; makes all preceding operations in this tx durable.
        Chinese: 记录事务提交；使本事务内此前所有操作在重启后生效。
        Japanese: トランザクションコミットを記録；本 tx 内の全操作を永続化。
        """
        line = f"COMMIT {tx_id}\n"
        with open(self._path, "a", encoding="utf-8") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())

    def log_insert(self, key: Any, value: Any, tx_id: Optional[int] = None) -> None:
        """
        English: Log INSERT before modifying; optionally under a transaction.
        Chinese: 在修改前记录 INSERT；可选归属于某事务。
        Japanese: 変更前に INSERT を記録；オプションでトランザクションに帰属。
        """
        val_str = value
        if isinstance(value, bytes):
            val_str = base64.b64encode(value).decode("ascii")
        prefix = f"TX{tx_id} " if tx_id is not None else ""
        line = f"{prefix}INSERT {key} {val_str}\n"
        with open(self._path, "a", encoding="utf-8") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())

    def log_delete(self, key: Any, tx_id: Optional[int] = None) -> None:
        """
        English: Log DELETE before modifying; optionally under a transaction.
        Chinese: 在修改前记录 DELETE；可选归属于某事务。
        Japanese: 変更前に DELETE を記録；オプションでトランザクションに帰属。
        """
        prefix = f"TX{tx_id} " if tx_id is not None else ""
        line = f"{prefix}DELETE {key}\n"
        with open(self._path, "a", encoding="utf-8") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())

    def log_checkpoint(self) -> None:
        """
        English: Log checkpoint for truncation point during recovery.
        Chinese: 记录检查点，供恢复时截断日志。
        Japanese: リカバリ時のログ truncate 用にチェックポイントを記録。
        """
        with open(self._path, "a", encoding="utf-8") as f:
            f.write("CHECKPOINT\n")
            f.flush()
            os.fsync(f.fileno())

    @staticmethod
    def replay(
        filepath: str | Path,
    ) -> Generator[tuple[str, Any, Optional[Any]], None, None]:
        """
        English: Replay WAL; only yield ops from committed transactions (atomic commit).
        Chinese: 重放 WAL；仅产出已提交事务的操作（原子提交语义）。
        Japanese: WAL をリプレイ；コミット済み tx の操作のみを yield（アトミックコミット）。
        """
        path = Path(filepath)
        if not path.exists():
            return
        # 收集每个事务的操作，仅当见到 COMMIT 时产出
        pending: dict[int, list[tuple[str, Any, Optional[Any]]]] = {}
        current_tx: Optional[int] = None
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line == "CHECKPOINT":
                    continue
                parts = line.split(None, 2)
                if not parts:
                    continue
                first = parts[0].upper()
                # TX_BEGIN tx_id
                if first == "TX_BEGIN" and len(parts) >= 2:
                    tx_id = int(parts[1])
                    current_tx = tx_id
                    pending[tx_id] = []
                    continue
                # COMMIT tx_id
                if first == "COMMIT" and len(parts) >= 2:
                    tx_id = int(parts[1])
                    for op, k, v in pending.get(tx_id, []):
                        yield (op, k, v)
                    pending.pop(tx_id, None)
                    if current_tx == tx_id:
                        current_tx = None
                    continue
                # TX{id} INSERT/DELETE
                if first.startswith("TX") and first[2:].isdigit():
                    tx_id = int(first[2:])
                    rest = (parts[1] + " " + parts[2]) if len(parts) >= 3 else (parts[1] if len(parts) >= 2 else "")
                    sub = rest.split(None, 2)
                    if len(sub) < 2:
                        continue
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
                    continue
                # 裸 INSERT/DELETE（向后兼容，无事务包装则立即产出）
                op = first
                if len(parts) < 2:
                    continue
                key_s = parts[1]
                key = int(key_s) if key_s.lstrip("-").isdigit() else key_s
                if op == "INSERT" and len(parts) >= 3:
                    insert_val: Any = parts[2]
                    try:
                        insert_val = base64.b64decode(insert_val.encode("ascii"))
                    except Exception:
                        pass
                    yield ("INSERT", key, insert_val)
                elif op == "DELETE":
                    yield ("DELETE", key, None)
