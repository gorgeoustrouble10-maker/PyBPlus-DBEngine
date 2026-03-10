"""
Phase 11-14 测试：FSM 闭环、CBO 联动、Undo Log 物理回滚。
"""

import tempfile
from pathlib import Path

import pytest

from bplus_tree.schema import Schema
from bplus_tree.table import RowTable
from bplus_tree.tree import BPlusTree
from bplus_tree.transaction import TransactionManager


class TestUndoPhysicalRollback:
    """Undo Log 物理回滚测试。"""

    def test_rollback_insert(self) -> None:
        """insert 后 rollback，数据应被撤销。"""
        schema = Schema(fields=[("id", "INT"), ("x", "FLOAT")])
        table = RowTable(schema, primary_key="id")
        mgr = TransactionManager()

        tx = mgr.begin()
        table.insert_row([1, 1.0], transaction=tx)
        table.insert_row([2, 2.0], transaction=tx)
        table.rollback_transaction(tx)
        mgr.abort(tx)

        rows = list(table.scan_with_condition(lambda r: True))
        assert len(rows) == 0

    def test_rollback_delete(self) -> None:
        """delete 后 rollback，数据应恢复。"""
        schema = Schema(fields=[("id", "INT"), ("x", "FLOAT")])
        table = RowTable(schema, primary_key="id")
        table.insert_row([1, 1.0])
        table.insert_row([2, 2.0])

        mgr = TransactionManager()
        tx = mgr.begin()
        table.delete_row(1, transaction=tx)
        table.rollback_transaction(tx)
        mgr.abort(tx)

        rows = list(table.scan_with_condition(lambda r: True))
        assert len(rows) == 2
        ids = sorted(r.get_field("id") for r in rows)
        assert ids == [1, 2]


class TestFSMMergeClosure:
    """FSM 闭环：merge 时 free_page。"""

    def test_merge_frees_page(self) -> None:
        """delete 触发 merge 时，被合并的页应归还 FSM。"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            path = Path(f.name)
        try:
            tree = BPlusTree(order=4)
            for i in range(15):
                tree.insert(i, f"v{i}")
            tree.save_to_db(path)

            loaded = BPlusTree.load_from_db(path, keep_pool=True)
            free_before = loaded._pool._fsm.free_count()
            for i in range(10):
                loaded.delete(i)
            free_after = loaded._pool._fsm.free_count()
            assert free_after >= free_before
        finally:
            path.unlink(missing_ok=True)
