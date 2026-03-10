"""
事务上下文与 MVCC-Lite：Transaction、ReadView、可见性判断、Undo Log 物理回滚。

English: Transaction context and MVCC-Lite; Transaction, ReadView, Undo Log for physical rollback.
Chinese: 事务上下文与 MVCC-Lite：事务、读视图、Undo Log 物理回滚。
Japanese: トランザクションコンテキストと MVCC-Lite：Transaction、ReadView、Undo Log による物理ロールバック。
"""

import struct
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Iterator, Optional

from bplus_tree.schema import Schema
from bplus_tree.table import Tuple

# 行头 (Record Header) 大小：transaction_id (8B) + roll_pointer (8B)
TX_ID_SIZE: int = 8
ROLL_PTR_SIZE: int = 8
RECORD_HEADER_SIZE: int = TX_ID_SIZE + ROLL_PTR_SIZE

# 预留：roll_pointer 为 0 表示无 Undo Log
ROLL_PTR_NONE: int = 0


@dataclass
class UndoRecord:
    """
    English: Single undo record; before_image for physical rollback.
    Chinese: 单条 Undo 记录；before_image 用于物理回滚。
    Japanese: 単一 Undo レコード；before_image で物理ロールバック。
    """

    op: str  # "INSERT" | "DELETE"
    key: Any
    before_image: Optional[bytes]  # 修改前值；INSERT 时为 None
    after_image: Optional[bytes]  # 修改后值；DELETE 时为 None


class TxState(Enum):
    """
    English: Transaction lifecycle state.
    Chinese: 事务生命周期状态。
    Japanese: トランザクションのライフサイクル状態。
    """

    ACTIVE = "ACTIVE"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"


class Transaction:
    """
    English: Transaction context with unique tx_id and state.
    Chinese: 事务上下文；持有唯一 tx_id 与状态（Active/Committed/Aborted）。
    Japanese: トランザクションコンテキスト；一意な tx_id と状態を保持。
    """

    def __init__(self, tx_id: int) -> None:
        """
        English: Create transaction with given id.
        Chinese: 用给定 id 创建事务。
        Japanese: 指定 id でトランザクションを作成します。
        """
        self._tx_id: int = tx_id
        self._state: TxState = TxState.ACTIVE
        self._undo_records: list[UndoRecord] = []

    @property
    def tx_id(self) -> int:
        """Unique transaction identifier."""
        return self._tx_id

    @property
    def state(self) -> TxState:
        """Current transaction state."""
        return self._state

    def log_insert_undo(self, key: Any, value: bytes) -> None:
        """
        English: Record INSERT for undo; rollback will delete key.
        Chinese: 记录 INSERT 的 Undo；回滚时删除 key。
        Japanese: INSERT の Undo を記録；ロールバック時に key を削除。
        """
        self._undo_records.append(
            UndoRecord(op="INSERT", key=key, before_image=None, after_image=value)
        )

    def log_delete_undo(self, key: Any, before_value: bytes) -> None:
        """
        English: Record DELETE for undo; rollback will restore before_value.
        Chinese: 记录 DELETE 的 Undo；回滚时恢复 before_value。
        Japanese: DELETE の Undo を記録；ロールバック時に before_value を復元。
        """
        self._undo_records.append(
            UndoRecord(op="DELETE", key=key, before_image=before_value, after_image=None)
        )

    def commit(self) -> None:
        """
        English: Mark transaction as committed; clear undo records.
        Chinese: 将事务标记为已提交；清空 Undo 记录。
        Japanese: トランザクションをコミット済みにマーク；Undo レコードをクリア。
        """
        if self._state != TxState.ACTIVE:
            raise RuntimeError(f"Cannot commit transaction in state {self._state}")
        self._state = TxState.COMMITTED
        self._undo_records = []

    def abort(self) -> None:
        """
        English: Mark transaction as aborted.
        Chinese: 将事务标记为已回滚。
        Japanese: トランザクションをアボート済みにマークします。
        """
        if self._state != TxState.ACTIVE:
            raise RuntimeError(f"Cannot abort transaction in state {self._state}")
        self._state = TxState.ABORTED

    def rollback(self, tree: Any) -> None:
        """
        English: Physical rollback: apply undo records in reverse, restore page data.
        Chinese: 物理回滚：逆序应用 Undo 记录，还原页数据。
        Japanese: 物理ロールバック：Undo レコードを逆順に適用し、ページデータを復元。

        Args:
            tree: BPlusTree to undo modifications on (must have delete/insert).
        """
        if self._state != TxState.ACTIVE:
            raise RuntimeError(f"Cannot rollback transaction in state {self._state}")
        for rec in reversed(self._undo_records):
            if rec.op == "INSERT":
                try:
                    tree.delete(rec.key)
                except KeyError:
                    pass
            elif rec.op == "DELETE" and rec.before_image is not None:
                tree.insert(rec.key, rec.before_image)
        self._undo_records = []
        self._state = TxState.ABORTED


class TransactionManager:
    """
    English: Allocates tx_ids and tracks committed transactions.
    Chinese: 分配 tx_id 并追踪已提交事务。
    Japanese: tx_id を割り当て、コミット済みトランザクションを追跡します。
    """

    def __init__(self) -> None:
        self._next_tx_id: int = 1
        self._committed: set[int] = set()
        self._active: set[int] = set()

    def begin(self) -> Transaction:
        """
        English: Start a new transaction.
        Chinese: 开始新事务。
        Japanese: 新規トランザクションを開始します。
        """
        tx_id = self._next_tx_id
        self._next_tx_id += 1
        self._active.add(tx_id)
        return Transaction(tx_id)

    def commit(self, tx: Transaction) -> None:
        """
        English: Commit transaction and update visibility.
        Chinese: 提交事务并更新可见性追踪。
        Japanese: トランザクションをコミットし、可視性追跡を更新。
        """
        tx.commit()
        self._active.discard(tx.tx_id)
        self._committed.add(tx.tx_id)

    def abort(self, tx: Transaction) -> None:
        """
        English: Abort transaction; idempotent if tx already aborted (e.g. after rollback).
        Chinese: 回滚事务；若已调用 rollback 则幂等。
        Japanese: トランザクションをアボート；rollback 済みなら冪等。
        """
        if tx._state == TxState.ACTIVE:
            tx.abort()
        self._active.discard(tx.tx_id)

    def is_committed(self, tx_id: int) -> bool:
        """Whether tx_id has committed."""
        return tx_id in self._committed

    def get_active_ids(self) -> frozenset[int]:
        """Current active (uncommitted) transaction ids."""
        return frozenset(self._active)


class ReadView:
    """
    English: Snapshot for MVCC visibility; defines which rows are visible to a reader.
    Chinese: MVCC 可见性快照；定义哪些行对读者可见。
    Japanese: MVCC 可視性スナップショット；読み手にどの行が見えるか定義。

    === 可见性判断 (Visibility) 深度解析 ===

    在多版本并发控制 (MVCC) 中，一条物理行可能被多个事务以不同版本写入。
    读操作需要决定：当前事务是否应该看到这条行的某一版本？

    本实现采用简化的 ReadView 模型（MVCC-Lite）：

    1. **快照时刻**：ReadView 创建时，记录：
       - creator_tx_id：创建该视图的事务 ID（即“读者”的事务 ID）
       - active_ids：创建时刻所有尚未提交的事务 ID 集合

    2. **可见性规则**：对于物理行上的 transaction_id（写该行的创建者事务 ID），
       该行对当前 ReadView 可见，当且仅当：

       a) 行的 tx_id 已提交（tx_id in committed_set）

       b) 行的 tx_id 在快照时刻尚未开始 或 已提交：
          - 若 tx_id < min(active_ids) 且 tx_id 不在 active_ids 中 → 可见
          - 若 tx_id 在 active_ids 中 → 不可见（创建者尚未提交）
          - 若 tx_id > creator_tx_id → 不可见（在读者之后开始，读者不应看到未来数据）

    3. **简化实现**：我们维护 committed 集合。行可见 iff：
       - row_tx_id in committed AND row_tx_id <= creator_tx_id
       即：行的创建事务已提交，且不晚于读者的“逻辑时间”。
    """

    def __init__(
        self,
        creator_tx_id: int,
        committed: set[int],
        active_ids: frozenset[int],
    ) -> None:
        """
        English: Create read view for visibility checks.
        Chinese: 创建读视图，用于可见性检查。
        Japanese: 可視性チェック用の ReadView を作成します。

        Args:
            creator_tx_id: Transaction ID of the reader.
            committed: Set of committed transaction IDs.
            active_ids: Set of active (uncommitted) transaction IDs at snapshot time.
        """
        self._creator_tx_id = creator_tx_id
        self._committed = frozenset(committed)
        self._active_ids = active_ids

    @property
    def creator_tx_id(self) -> int:
        """读者事务 ID。"""
        return self._creator_tx_id

    def is_visible(self, row_tx_id: int) -> bool:
        """
        English: True if row created by row_tx_id is visible to this read view.
        Chinese: 若 row_tx_id 创建的行对此读视图可见则返回 True。
        Japanese: row_tx_id が作成した行がこの ReadView に可視なら True。

        可见性判断核心逻辑：
        - 若创建该行的事务尚未提交（在 active 中），则不可见
        - 若创建该行的事务已提交，且其 tx_id 不晚于本读者的 tx_id，则可见
        """
        # 1. 行的创建事务必须已提交
        if row_tx_id not in self._committed:
            return False
        # 2. 行不能在“未来”：创建者 tx_id 不能晚于读者的 tx_id
        if row_tx_id > self._creator_tx_id:
            return False
        # 3. 创建者不能在快照时刻仍处于活跃状态（双重保险；已提交则必不在 active）
        if row_tx_id in self._active_ids:
            return False
        return True


# ---------------------------------------------------------------------------
# 行头 (Record Header) 序列化 / 反序列化
# ---------------------------------------------------------------------------


def pack_record_header(tx_id: int, roll_pointer: int = ROLL_PTR_NONE) -> bytes:
    """
    English: Pack record header: tx_id (8B) + roll_pointer (8B).
    Chinese: 打包行头：transaction_id (8B) + roll_pointer (8B)。
    Japanese: レコードヘッダをパック：tx_id (8B) + roll_pointer (8B)。
    """
    return struct.pack("<qq", tx_id, roll_pointer)


def unpack_record_header(raw: bytes) -> tuple[int, int]:
    """
    English: Unpack first 16 bytes to (tx_id, roll_pointer).
    Chinese: 解包前 16 字节为 (transaction_id, roll_pointer)。
    Japanese: 先頭 16 バイトを (tx_id, roll_pointer) にアンパック。
    """
    if len(raw) < RECORD_HEADER_SIZE:
        # 兼容无头部的旧格式：视为 tx_id=1 (已提交), roll_ptr=0
        return (1, ROLL_PTR_NONE)
    tx_id, roll_ptr = struct.unpack_from("<qq", raw, 0)
    return (tx_id, roll_ptr)


def serialize_row_with_header(
    schema: Schema,
    values: list[Any],
    tx_id: int,
    roll_pointer: int = ROLL_PTR_NONE,
) -> bytes:
    """
    English: Serialize row with record header (tx_id + roll_ptr) prefix.
    Chinese: 序列化行，前缀为行头（tx_id + roll_ptr）。
    Japanese: 行頭（tx_id + roll_ptr）をプレフィックスして行をシリアライズ。
    """
    header = pack_record_header(tx_id, roll_pointer)
    payload: bytes = schema.serialize_row(values)
    return header + payload


def deserialize_row_with_header(
    schema: Schema,
    raw: bytes,
) -> tuple[list[Any], int, int]:
    """
    English: Deserialize row; return (values, tx_id, roll_pointer).
    Chinese: 反序列化行；返回 (值列表, transaction_id, roll_pointer)。
    Japanese: 行をデシリアライズ；(値リスト, tx_id, roll_pointer) を返します。
    """
    if len(raw) < RECORD_HEADER_SIZE:
        # 无头部：整块作为 payload，tx_id=1 表示“ legacy 已提交”
        values = schema.deserialize_row(raw)
        return (values, 1, ROLL_PTR_NONE)
    tx_id, roll_ptr = unpack_record_header(raw)
    payload = raw[RECORD_HEADER_SIZE:]
    values = schema.deserialize_row(payload)
    return (values, tx_id, roll_ptr)


def create_read_view(
    tx_manager: TransactionManager,
    creator_tx_id: int,
) -> ReadView:
    """
    English: Create ReadView for given reader transaction.
    Chinese: 为给定读者事务创建 ReadView。
    Japanese: 指定した読み手トランザクション用の ReadView を作成します。
    """
    return ReadView(
        creator_tx_id=creator_tx_id,
        committed=set(tx_manager._committed),
        active_ids=tx_manager.get_active_ids(),
    )
