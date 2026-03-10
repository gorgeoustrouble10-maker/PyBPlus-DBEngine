"""
持久化表与二级索引测试；覆盖关闭重开后按索引查询。
"""

import tempfile
from pathlib import Path

import pytest

from bplus_tree.database import PersistentTable
from bplus_tree.schema import Schema


class TestPersistentTable:
    """持久化表与 open/create 测试。"""

    def test_create_open_insert_flush_reopen(self) -> None:
        """create -> insert -> flush -> 关闭 -> reopen -> 数据仍在。"""
        schema = Schema(fields=[("id", "INT"), ("name", "VARCHAR(32)"), ("score", "FLOAT")])
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            path = Path(f.name)
        try:
            tbl = PersistentTable.create(schema, "id", path)
            tbl.insert_row([1, "Alice", 95.5])
            tbl.insert_row([2, "Bob", 88.0])
            tbl.flush()

            tbl2 = PersistentTable.open(path)
            rows = list(tbl2.scan_with_condition(lambda r: True))
            assert len(rows) == 2
            names = [r.get_field("name") for r in rows]
            assert "Alice" in names
            assert "Bob" in names
        finally:
            path.unlink(missing_ok=True)

    def test_reopen_get_by_index(self) -> None:
        """关闭并重开数据库后依然能按索引查询。"""
        schema = Schema()
        schema.add_field("id", "INT")
        schema.add_field("name", "VARCHAR(32)")
        schema.add_field("score", "FLOAT")

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            path = Path(f.name)
        try:
            tbl = PersistentTable.create(schema, "id", path)
            tbl.insert_row([1, "Alice", 95.5])
            tbl.insert_row([2, "Bob", 88.0])
            tbl.insert_row([3, "Alice", 92.0])
            tbl.create_index("name")
            tbl.flush()

            tbl2 = PersistentTable.open(path)
            by_name = list(tbl2.get_by_index("name", "Alice"))
            assert len(by_name) == 2
            ids = sorted(r.get_field("id") for r in by_name)
            assert ids == [1, 3]
        finally:
            path.unlink(missing_ok=True)

    def test_secondary_index_sync_on_insert(self) -> None:
        """insert 时二级索引自动同步。"""
        schema = Schema(fields=[("id", "INT"), ("city", "VARCHAR(16)")])
        tbl = PersistentTable(schema, "id")
        tbl.insert_row([1, "Beijing"])
        tbl.insert_row([2, "Shanghai"])
        tbl.create_index("city")
        tbl.insert_row([3, "Beijing"])

        beijing = list(tbl.get_by_index("city", "Beijing"))
        assert len(beijing) == 2
        assert {r.get_field("id") for r in beijing} == {1, 3}

    def test_secondary_index_sync_on_delete(self) -> None:
        """delete_row 时二级索引自动同步。"""
        schema = Schema(fields=[("id", "INT"), ("x", "VARCHAR(8)")])
        tbl = PersistentTable(schema, "id")
        tbl.insert_row([1, "A"])
        tbl.insert_row([2, "A"])
        tbl.create_index("x")
        tbl.delete_row(1)

        remaining = list(tbl.get_by_index("x", "A"))
        assert len(remaining) == 1
        assert remaining[0].get_field("id") == 2

    def test_open_nonexistent_raises(self) -> None:
        """open 不存在的文件抛出 FileNotFoundError。"""
        with pytest.raises(FileNotFoundError, match="not found"):
            PersistentTable.open("/nonexistent/path/data.db")
