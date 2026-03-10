"""
数据库上下文：多表管理、Catalog、恢复、统一持久化 (BufferPool + WAL)。

English: Database context; multi-table management, Catalog, recovery, unified persistence.
Chinese: 数据库上下文：多表管理、Catalog、恢复、统一持久化。
Japanese: データベースコンテキスト：マルチテーブル管理、Catalog、リカバリ、統一永続化。
"""

import os
from pathlib import Path
from typing import Any, Optional

from bplus_tree.background import BackgroundWriter, truncate_wal_after_checkpoint
from bplus_tree.catalog import Catalog
from bplus_tree.errors import TableInUseError, UnknownTableError
from bplus_tree.logging import WriteAheadLog
from bplus_tree.schema import Schema
from bplus_tree.storage import BufferPool
from bplus_tree.table import RowTable
from bplus_tree.tree import BPlusTree


def run_recovery(data_dir: Path, tables: dict[str, RowTable]) -> None:
    """
    English: Replay WAL; per-table wal_{name}.log, else global wal.log into first table.
    Chinese: 重放 WAL；每表 wal_{name}.log，否则用 wal.log 重放到第一个表。
    Japanese: WAL をリプレイ；wal_{name}.log または wal.log を先頭テーブルに。
    """
    has_per_table = False
    for name, table in tables.items():
        per_wal = data_dir / f"wal_{name}.log"
        if per_wal.exists():
            has_per_table = True
            for op, key, value in WriteAheadLog.replay(per_wal):
                try:
                    if op == "INSERT" and value is not None:
                        table._tree.insert(key, value)
                    elif op == "DELETE":
                        table._tree.delete(key)
                except (KeyError, Exception):
                    pass
    if not has_per_table and tables:
        global_wal = data_dir / "wal.log"
        if global_wal.exists():
            target = next(iter(tables.values()))
            for op, key, value in WriteAheadLog.replay(global_wal):
                try:
                    if op == "INSERT" and value is not None:
                        target._tree.insert(key, value)
                    elif op == "DELETE":
                        target._tree.delete(key)
                except (KeyError, Exception):
                    pass


class DatabaseContext:
    """
    English: Holds tables, catalog, data_dir; unified persistence via BufferPool + WAL.
    Chinese: 持有 tables、catalog、data_dir；通过 BufferPool + WAL 统一持久化。
    Japanese: tables、catalog、data_dir を保持；BufferPool + WAL で統一永続化。
    """

    def __init__(
        self,
        data_dir: str | Path,
        wal_filename: str = "wal.log",
    ) -> None:
        self._data_dir = Path(data_dir)
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._catalog = Catalog(self._data_dir / "catalog.json")
        self._catalog.load()
        self._tables: dict[str, RowTable] = {}
        self._pools: dict[str, BufferPool] = {}
        self._wal_path = self._data_dir / wal_filename
        self._wal_filename = wal_filename
        self._primary_table: Optional[str] = None
        self._background_writer: Optional[BackgroundWriter] = None

    def _create_table_internal(
        self,
        name: str,
        schema: Schema,
        primary_key: str,
    ) -> RowTable:
        """
        English: Create RowTable with BufferPool-backed B+ tree and WAL.
        Chinese: 创建带 BufferPool 与 WAL 的 RowTable。
        Japanese: BufferPool と WAL 付き RowTable を作成。
        """
        db_path = self._data_dir / f"{name}.db"
        wal_path = self._data_dir / f"wal_{name}.log"
        tree = BPlusTree.load_from_db(db_path, keep_pool=True)
        tree._wal = WriteAheadLog(wal_path)
        pool = tree._pool
        if pool is not None:
            self._pools[name] = pool
        table = RowTable(schema, primary_key, tree=tree)
        self._tables[name] = table
        return table

    def _flush_all_pools(self) -> int:
        """Flush all BufferPools' dirty pages; return total count."""
        total = 0
        for pool in self._pools.values():
            total += pool.flush_dirty_pages()
        return total

    def _start_background_writer(self) -> None:
        """Start BackgroundWriter; use wrapper to flush all pools."""
        if self._background_writer is not None or not self._pools:
            return

        class _MultiPoolFlusher:
            def __init__(self, pools: dict[str, BufferPool]) -> None:
                self._pools = pools

            def flush_dirty_pages(self) -> int:
                return sum(p.flush_dirty_pages() for p in self._pools.values())

        flusher = _MultiPoolFlusher(self._pools)
        self._background_writer = BackgroundWriter(flusher, interval_sec=1.0)
        self._background_writer.start()

    def load_tables(self) -> None:
        """
        English: Load all tables from catalog; BufferPool-backed, start BackgroundWriter.
        Chinese: 从 Catalog 加载表；BufferPool 支撑，启动 BackgroundWriter。
        Japanese: Catalog からテーブルをロード；BufferPool で BackgroundWriter を起動。
        """
        for name in self._catalog.list_tables():
            schema, pk = self._catalog.get_schema_and_pk(name)
            self._create_table_internal(name, schema, pk)
        if self._tables and self._primary_table is None:
            self._primary_table = self._catalog.list_tables()[0]
        self._start_background_writer()

    def run_recovery(self) -> None:
        """
        English: Replay WAL into tables if wal.log or wal_{name}.log exists.
        Chinese: 若 wal.log 或 wal_{name}.log 存在则重放到对应表。
        Japanese: wal.log または wal_{name}.log が存在すればリプレイ。
        """
        run_recovery(self._data_dir, self._tables)
        for t in self._tables.values():
            t.refresh_stats()

    def create_table(
        self,
        name: str,
        schema: Schema,
        primary_key: str,
    ) -> RowTable:
        """Create table, add to catalog, persist; BufferPool-backed."""
        self._catalog.add_table(name, schema, primary_key)
        table = self._create_table_internal(name, schema, primary_key)
        if self._primary_table is None:
            self._primary_table = name
        self._start_background_writer()
        return table

    def get_table(self, name: str) -> RowTable:
        """Get table by name."""
        if name not in self._tables:
            raise UnknownTableError(name)
        return self._tables[name]

    def get_tables(self) -> dict[str, RowTable]:
        """All tables."""
        return dict(self._tables)

    def get_primary_table(self) -> Optional[RowTable]:
        """Primary table for legacy single-table operations."""
        if self._primary_table:
            return self._tables.get(self._primary_table)
        return next(iter(self._tables.values())) if self._tables else None

    def drop_table(self, name: str) -> None:
        """
        English: Drop table; reclaim resources (WAL file), update catalog.
        Chinese: 删除表；回收资源（WAL 文件），更新 Catalog。
        Japanese: テーブルを削除；リソース（WAL ファイル）を回収、Catalog を更新。

        Pre-condition: Caller should hold exclusive access; no active transactions on this table.
        Safety: Catches PermissionError when file is in use, raises TableInUseError.
        """
        if name not in self._tables:
            raise UnknownTableError(name)
        table = self._tables.pop(name)
        if self._primary_table == name:
            self._primary_table = next(iter(self._tables.keys()), None) if self._tables else None
        self._catalog.remove_table(name)
        wal_path = self._data_dir / f"wal_{name}.log"
        if wal_path.exists():
            try:
                wal_path.unlink()
            except PermissionError:
                self._tables[name] = table
                self._catalog.add_table(name, table._schema, table._pk)
                raise TableInUseError(name)
        db_path = self._data_dir / f"{name}.db"
        idx_path = self._data_dir / f"{name}.idx"
        for p in (db_path, idx_path):
            if p.exists():
                try:
                    os.remove(p)
                except PermissionError:
                    pass

    def checkpoint_all(self) -> None:
        """
        English: Flush BufferPool, log CHECKPOINT, truncate WAL (unified persistence).
        Chinese: 刷写 BufferPool，记录 CHECKPOINT，截断 WAL（统一持久化）。
        Japanese: BufferPool をフラッシュ、CHECKPOINT 記録、WAL を truncate。
        """
        for name, table in list(self._tables.items()):
            tree = table._tree
            pool = getattr(tree, "_pool", None)
            if pool is not None:
                from bplus_tree.tree import _persist_tree_to_pool

                _persist_tree_to_pool(tree._root, pool, tree._order)
                pool.flush()
            wal = getattr(tree, "_wal", None)
            if wal is not None:
                wal.log_checkpoint()
            truncate_wal_after_checkpoint(self._data_dir / f"wal_{name}.log")
