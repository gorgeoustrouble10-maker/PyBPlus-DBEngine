"""
元数据持久化：Catalog 存储表定义，支持重启后恢复。

English: Metadata persistence; Catalog stores table definitions for recovery.
Chinese: 元数据持久化：Catalog 存储表定义，支持重启后恢复。
Japanese: メタデータ永続化；Catalog でテーブル定義を保存、再起動後に復元。
"""

import json
from pathlib import Path
from typing import Any

from bplus_tree.errors import TableExistsError, UnknownTableError
from bplus_tree.schema import Schema
from bplus_tree.table import RowTable
from bplus_tree.tree import BPlusTree


def _schema_to_json(schema: Schema) -> list[tuple[str, str]]:
    """Serialize schema to JSON-safe format."""
    return list(schema._fields)


def _schema_from_json(fields: list[list[str]]) -> Schema:
    """Deserialize schema from JSON."""
    return Schema(fields=[(f[0], f[1]) for f in fields])


class Catalog:
    """
    English: In-memory catalog with JSON persistence; table_name -> (Schema, primary_key).
    Chinese: 内存 Catalog，JSON 持久化；table_name -> (Schema, primary_key)。
    Japanese: メモリカタログ、JSON 永続化；table_name -> (Schema, primary_key)。
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._tables: dict[str, tuple[list[tuple[str, str]], str]] = {}

    def load(self) -> None:
        """
        English: Load catalog from disk.
        Chinese: 从磁盘加载 Catalog。
        Japanese: ディスクから Catalog をロード。
        """
        if not self._path.exists():
            return
        with open(self._path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for name, meta in data.get("tables", {}).items():
            fields = [tuple(f) for f in meta["fields"]]
            pk = meta["primary_key"]
            self._tables[name] = (fields, pk)

    def save(self) -> None:
        """
        English: Persist catalog to disk.
        Chinese: 将 Catalog 持久化到磁盘。
        Japanese: Catalog をディスクに永続化。
        """
        data = {
            "tables": {
                name: {"fields": list(fields), "primary_key": pk}
                for name, (fields, pk) in self._tables.items()
            }
        }
        with open(self._path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def add_table(
        self,
        name: str,
        schema: Schema,
        primary_key: str,
    ) -> None:
        """
        English: Register table; raise if exists.
        Chinese: 注册表；若已存在则抛出。
        Japanese: テーブルを登録；存在すれば例外。
        """
        if name in self._tables:
            raise TableExistsError(name)
        self._tables[name] = (list(schema._fields), primary_key)
        self.save()

    def get_schema_and_pk(self, name: str) -> tuple[Schema, str]:
        """
        English: Get schema and primary key for table.
        Chinese: 获取表的 Schema 与主键。
        Japanese: テーブルの Schema と主キーを取得。
        """
        if name not in self._tables:
            raise UnknownTableError(name)
        fields, pk = self._tables[name]
        return Schema(fields=[(f[0], f[1]) for f in fields]), pk

    def list_tables(self) -> list[str]:
        """List all table names."""
        return list(self._tables.keys())

    def has_table(self, name: str) -> bool:
        """Whether table exists."""
        return name in self._tables

    def remove_table(self, name: str) -> None:
        """
        English: Remove table from catalog; persist.
        Chinese: 从 Catalog 删除表并持久化。
        Japanese: Catalog からテーブルを削除し永続化。
        """
        if name not in self._tables:
            raise UnknownTableError(name)
        del self._tables[name]
        self.save()
