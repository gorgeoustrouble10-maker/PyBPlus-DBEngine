"""
事务与 MVCC-Lite 测试：Transaction、ReadView、可见性判断、WAL 原子提交。
"""

import tempfile
from pathlib import Path

import pytest

from bplus_tree.logging import WriteAheadLog
from bplus_tree.schema import Schema
from bplus_tree.table import RowTable, Tuple
from bplus_tree.transaction import (
    ReadView,
    Transaction,
    TransactionManager,
    TxState,
)


class TestTransaction:
    """Transaction 与状态测试。"""

    def test_begin_commit(self) -> None:
        """begin -> commit 后状态为 COMMITTED。"""
        mgr = TransactionManager()
        tx = mgr.begin()
        assert tx.state == TxState.ACTIVE
        mgr.commit(tx)
        assert tx.state == TxState.COMMITTED
        assert mgr.is_committed(tx.tx_id)

    def test_begin_abort(self) -> None:
        """begin -> abort 后状态为 ABORTED。"""
        mgr = TransactionManager()
        tx = mgr.begin()
        mgr.abort(tx)
        assert tx.state == TxState.ABORTED
        assert not mgr.is_committed(tx.tx_id)


class TestRecordHeader:
    """行头（tx_id + roll_ptr）测试。"""

    def test_tuple_has_tx_id_from_raw(self) -> None:
        """从带行头的 raw 创建的 Tuple 有正确的 tx_id。"""
        schema = Schema(fields=[("id", "INT")])
        row = Tuple(schema, values=[1])
        raw = row.to_bytes(tx_id=42, roll_pointer=0)
        t = Tuple(schema, raw=raw)
        assert t.tx_id == 42


class TestReadViewVisibility:
    """ReadView 可见性判断测试。"""

    def test_committed_row_visible(self) -> None:
        """已提交事务写入的行对 ReadView 可见。"""
        mgr = TransactionManager()
        tx = mgr.begin()
        mgr.commit(tx)
        rv = ReadView(
            creator_tx_id=tx.tx_id,
            committed=set(mgr._committed),
            active_ids=mgr.get_active_ids(),
        )
        assert rv.is_visible(tx.tx_id)

    def test_uncommitted_row_not_visible(self) -> None:
        """未提交事务写入的行对 ReadView 不可见。"""
        mgr = TransactionManager()
        tx = mgr.begin()
        rv = ReadView(
            creator_tx_id=tx.tx_id,
            committed=set(mgr._committed),
            active_ids=mgr.get_active_ids(),
        )
        assert not rv.is_visible(tx.tx_id)


class TestRowTableWithTransaction:
    """带事务的 RowTable insert/scan 测试。"""

    def test_insert_with_tx_and_scan_with_read_view(self) -> None:
        """带事务插入后，只有 commit 后对 ReadView 可见。"""
        schema = Schema(fields=[("id", "INT"), ("x", "VARCHAR(8)")])
        mgr = TransactionManager()
        table = RowTable(schema, "id", tx_manager=mgr)

        tx1 = mgr.begin()
        table.insert_row([1, "a"], transaction=tx1)
        mgr.commit(tx1)

        rv = ReadView(
            creator_tx_id=tx1.tx_id,
            committed=set(mgr._committed),
            active_ids=mgr.get_active_ids(),
        )
        rows = list(table.scan_with_condition(lambda r: True, read_view=rv))
        assert len(rows) == 1
        assert rows[0].get_field("id") == 1

    def test_uncommitted_insert_not_visible(self) -> None:
        """未提交的插入对 ReadView 不可见。"""
        schema = Schema(fields=[("id", "INT")])
        mgr = TransactionManager()
        table = RowTable(schema, "id", tx_manager=mgr)

        tx1 = mgr.begin()
        table.insert_row([1], transaction=tx1)
        # 不 commit

        rv = ReadView(
            creator_tx_id=2,
            committed=set(),
            active_ids=mgr.get_active_ids(),
        )
        rows = list(table.scan_with_condition(lambda r: True, read_view=rv))
        assert len(rows) == 0


class TestWalAtomicCommit:
    """WAL 原子提交测试。"""

    def test_uncommitted_tx_not_replayed(self) -> None:
        """无 COMMIT 的事务在 replay 时不会产出其操作。"""
        with tempfile.NamedTemporaryFile(suffix=".log", delete=False) as f:
            path = Path(f.name)
        try:
            wal = WriteAheadLog(path)
            wal.log_tx_begin(1)
            wal.log_insert(10, b"val10", tx_id=1)
            # 不写 COMMIT 1
            ops = list(WriteAheadLog.replay(path))
            assert len(ops) == 0
        finally:
            path.unlink(missing_ok=True)

    def test_committed_tx_replayed(self) -> None:
        """有 COMMIT 的事务在 replay 时产出其操作。"""
        with tempfile.NamedTemporaryFile(suffix=".log", delete=False) as f:
            path = Path(f.name)
        try:
            wal = WriteAheadLog(path)
            wal.log_tx_begin(1)
            wal.log_insert(10, b"val10", tx_id=1)
            wal.log_commit(1)
            ops = list(WriteAheadLog.replay(path))
            assert len(ops) == 1
            assert ops[0] == ("INSERT", 10, b"val10")
        finally:
            path.unlink(missing_ok=True)
