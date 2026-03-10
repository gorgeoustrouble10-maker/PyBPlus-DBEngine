"""
后台刷脏与 Checkpoint：BackgroundWriter、WAL 截断。

English: Background flush and checkpoint; BackgroundWriter, WAL truncation.
Chinese: 后台刷脏与 Checkpoint；BackgroundWriter、WAL 截断。
Japanese: バックグラウンドフラッシュとチェックポイント；BackgroundWriter、WAL  truncation。
"""

import os
import threading
from pathlib import Path
from typing import Optional

from bplus_tree.logging import WriteAheadLog
from bplus_tree.storage import BufferPool


class BackgroundWriter:
    """
    English: Daemon thread that periodically flushes dirty pages from BufferPool.
    Chinese: 后台守护线程，定时将 Buffer Pool 脏页刷盘。
    Japanese: バッファプールのダーティページを定期的にフラッシュするデーモンスレッド。
    """

    def __init__(
        self,
        pool: BufferPool,
        interval_sec: float = 1.0,
    ) -> None:
        """
        English: Create background writer; start() to begin flushing.
        Chinese: 创建后台刷写器；start() 启动。
        Japanese: バックグラウンドライターを作成；start() で開始。
        """
        self._pool = pool
        self._interval = interval_sec
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """
        English: Start background flush thread (daemon).
        Chinese: 启动后台刷脏线程（守护）。
        Japanese: バックグラウンドフラッシュスレッドを開始（デーモン）。
        """
        if self._thread is not None and self._thread.is_alive():
            return

        def _run() -> None:
            while not self._stop.wait(self._interval):
                self._pool.flush_dirty_pages()

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """
        English: Stop background writer.
        Chinese: 停止后台刷写器。
        Japanese: バックグラウンドライターを停止。
        """
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self._interval * 2)
            self._thread = None


def do_checkpoint(
    pool: BufferPool,
    wal: Optional[WriteAheadLog] = None,
) -> None:
    """
    English: Flush all dirty pages, log CHECKPOINT; optionally truncate WAL.
    Chinese: 刷写全部脏页，记录 CHECKPOINT；可选截断 WAL。
    Japanese: 全ダーティページをフラッシュし CHECKPOINT を記録；オプションで WAL を truncate。
    """
    pool.flush()
    if wal is not None:
        wal.log_checkpoint()


def truncate_wal_after_checkpoint(wal_path: str | Path) -> None:
    """
    English: Truncate WAL file, keeping only content after last CHECKPOINT.
    Chinese: 截断 WAL 文件，仅保留最后 CHECKPOINT 之后的内容。
    Japanese: WAL を truncate；最後の CHECKPOINT 以降のみ残す。
    """
    path = Path(wal_path)
    if not path.exists():
        return
    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    last_cp = -1
    for i, line in enumerate(lines):
        if line.strip() == "CHECKPOINT":
            last_cp = i
    if last_cp < 0:
        return
    content_after = "".join(lines[last_cp + 1 :])
    with open(path, "w", encoding="utf-8") as f:
        f.write(content_after)
        f.flush()
        os.fsync(f.fileno())
