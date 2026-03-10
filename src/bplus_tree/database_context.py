"""
数据库上下文：多表管理、Catalog、恢复。

English: Database context; multi-table management, Catalog, recovery.
Chinese: 数据库上下文：多表管理、Catalog、恢复。
Japanese: データベースコンテキスト：マルチテーブル管理、Catalog、リカバリ。
"""

import os
from pathlib import Path
from typing import Any, Optional

from bplus_tree.background import truncate_wal_after_checkpoint
from bplus_tree.catalog import Catalog
from bplus_tree.errors import TableInUseError, UnknownTableError
from bplus_tree.logging import WriteAheadLog
from bplus_tree.schema import Schema
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
    English: Holds tables, catalog, data_dir; supports CREATE TABLE and recovery.
    Chinese: 持有 tables、catalog、data_dir；支持 CREATE TABLE 与恢复。
    Japanese: tables、catalog、data_dir を保持；CREATE TABLE とリカバリをサポート。
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
        self._wal_path = self._data_dir / wal_filename
        self._wal_filename = wal_filename
        self._primary_table: Optional[str] = None

    def _create_table_internal(
        self,
        name: str,
        schema: Schema,
        primary_key: str,
    ) -> RowTable:
        """Create RowTable with WAL enabled for this table's tree."""
        wal_path = self._data_dir / f"wal_{name}.log"
        tree = BPlusTree(order=16, wal_path=wal_path)
        table = RowTable(schema, primary_key, tree=tree)
        self._tables[name] = table
        return table

    def load_tables(self) -> None:
        """
        English: Load all tables from catalog into memory.
        Chinese: 从 Catalog 加载所有表到内存。
        Japanese: Catalog から全テーブルをメモリにロード。
        """
        for name in self._catalog.list_tables():
            schema, pk = self._catalog.get_schema_and_pk(name)
            self._create_table_internal(name, schema, pk)
        if self._tables and self._primary_table is None:
            self._primary_table = self._catalog.list_tables()[0]

    def run_recovery(self) -> None:
        """
        English: Replay WAL into tables if wal.log or wal_{name}.log exists.
        Chinese: 若 wal.log 或 wal_{name}.log 存在则重放到对应表。
        Japanese: wal.log または wal_{name}.log が存在すればリプレイ。
        """
        run_recovery(self._data_dir, self._tables)

    def create_table(
        self,
        name: str,
        schema: Schema,
        primary_key: str,
    ) -> RowTable:
        """Create table, add to catalog, persist."""
        self._catalog.add_table(name, schema, primary_key)
        table = self._create_table_internal(name, schema, primary_key)
        if self._primary_table is None:
            self._primary_table = name
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
        English: Checkpoint all table WALs; fsync semantics via log_checkpoint, then truncate.
        Chinese: 对所有表 WAL 执行 Checkpoint；通过 log_checkpoint 落盘，再截断。
        Japanese: 全テーブル WAL をチェックポイント；log_checkpoint で永続化後 truncate。
        """
        for name, table in list(self._tables.items()):
            wal = getattr(table._tree, "_wal", None)
            if wal is not None:
                wal.log_checkpoint()
            truncate_wal_after_checkpoint(self._data_dir / f"wal_{name}.log")
