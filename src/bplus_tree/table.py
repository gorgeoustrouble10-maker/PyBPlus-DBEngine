"""
表与行存储：Tuple、insert_row、scan_with_condition。

English: Table and row storage; Tuple, insert_row, scan with filter.
Chinese: 表与行存储：元组、按行插入、条件扫描。
Japanese: テーブルと行ストレージ：タプル、行挿入、条件付きスキャン。
"""

import struct
from typing import Any, Callable, Iterator, Optional

from bplus_tree.schema import Schema
from bplus_tree.tree import BPlusTree


class Tuple:
    """
    English: A row/tuple; holds values per schema with get_field(name).
    Chinese: 元组/行；按模式存储值，支持 get_field(name)。
    Japanese: 行/タプル；スキーマに従い値を保持、get_field(name) をサポート。
    """

    def __init__(
        self,
        schema: Schema,
        values: Optional[list[Any]] = None,
        raw: Optional[bytes] = None,
    ) -> None:
        """
        English: Create tuple from list or raw bytes (with or without record header).
        Chinese: 从列表或原始字节创建元组（支持带/不带行头的格式）。
        Japanese: リストまたは生バイトからタプルを作成（レコードヘッダ有無対応）。

        Args:
            schema: Row schema.
            values: Python list [v1, v2, ...].
            raw: Serialized bytes (optionally with 16B header: tx_id + roll_ptr).
        """
        self._schema = schema
        self._tx_id: int = 0
        self._roll_ptr: int = 0
        if values is not None:
            self._values = schema.deserialize_row(schema.serialize_row(values))
        elif raw is not None:
            # 兼容带行头与无行头格式：RHD1 魔数表示新格式（4B magic + tx_id 8B + roll_ptr 8B）
            if len(raw) >= 20 and raw[:4] == b"RHD1":
                self._tx_id = struct.unpack_from("<q", raw, 4)[0]
                self._roll_ptr = struct.unpack_from("<q", raw, 12)[0]
                payload = raw[20:]
            else:
                # 无魔数：整块为 payload，视为已提交（tx_id=1）
                payload = raw
                self._tx_id = 1
                self._roll_ptr = 0
            self._values = schema.deserialize_row(payload)
        else:
            raise ValueError("Provide values or raw")

    def get_field(self, name: str) -> Any:
        """
        English: Get value by field name.
        Chinese: 根据字段名获取值。
        Japanese: フィールド名で値を取得します。
        """
        names = self._schema.field_names()
        if name not in names:
            raise KeyError(f"Field '{name}' not in schema")
        idx = names.index(name)
        return self._values[idx]

    def to_bytes(
        self,
        tx_id: int = 1,
        roll_pointer: int = 0,
    ) -> bytes:
        """
        English: Serialize to binary bytes with record header (tx_id + roll_ptr).
        Chinese: 序列化为二进制，带行头（tx_id + roll_ptr）。
        Japanese: バイナリにシリアライズ、行頭（tx_id + roll_ptr）付き。

        Args:
            tx_id: Transaction ID for record header (default 1 = legacy committed).
            roll_pointer: Roll pointer for Undo Log (default 0 = none).
        """
        header = struct.pack("<4sqq", b"RHD1", tx_id, roll_pointer)
        payload: bytes = self._schema.serialize_row(self._values)
        return header + payload

    @property
    def tx_id(self) -> int:
        """Row creator's transaction ID (from record header)."""
        return self._tx_id

    def as_list(self) -> list[Any]:
        """
        English: Return all values as list.
        Chinese: 以列表形式返回所有值。
        Japanese: 全値をリスト形式で返します。
        """
        return list(self._values)

    def as_dict(self) -> dict[str, Any]:
        """
        English: Return mapping of field names to values.
        Chinese: 以字典形式返回字段名到值的映射。
        Japanese: フィールド名から値へのマッピングを返します。
        """
        return dict(zip(self._schema.field_names(), self._values))


class RowTable:
    """
    English: Table backed by BPlusTree; stores binary Tuple, supports insert_row and scan_with_condition.
    Chinese: 基于 BPlusTree 的表；存二进制元组，支持 insert_row 与 scan_with_condition。
    Japanese: BPlusTree をバックエンドとするテーブル；バイナリタプルを格納、insert_row と scan_with_condition をサポート。
    """

    def __init__(
        self,
        schema: Schema,
        primary_key: str,
        tree: Optional[BPlusTree] = None,
        order: int = 16,
        tx_manager: Optional[Any] = None,
    ) -> None:
        """
        English: Create table with schema and primary key field.
        Chinese: 用模式与主键字段创建表。
        Japanese: スキーマと主キーでテーブルを作成します。

        Args:
            schema: Row schema.
            primary_key: Field name used as B+ tree key (must be INT or comparable).
            tree: Optional existing BPlusTree; if None, creates new one.
            order: B+ tree order when creating new tree.
            tx_manager: Optional TransactionManager for MVCC.
        """
        self._schema = schema
        self._pk = primary_key
        if primary_key not in schema.field_names():
            raise ValueError(f"Primary key '{primary_key}' not in schema")
        self._pk_idx = schema.field_names().index(primary_key)
        self._tree = tree if tree is not None else BPlusTree(order=order)
        self._tx_manager = tx_manager
        self._total_rows: int = 0
        self._index_unique_counts: dict[str, int] = {}

    def insert_row(
        self,
        tuple_data: list[Any],
        transaction: Optional[Any] = None,
    ) -> None:
        """
        English: Insert a row; optionally with transaction for MVCC and Undo Log.
        Chinese: 插入一行；可选传入事务以写入行头 tx_id 并记录 Undo。
        Japanese: 行を挿入；オプションでトランザクションを渡し tx_id と Undo を記録。
        """
        if len(tuple_data) != len(self._schema):
            raise ValueError(
                f"Row has {len(tuple_data)} values, schema expects {len(self._schema)}"
            )
        key = tuple_data[self._pk_idx]
        row = Tuple(self._schema, values=tuple_data)
        tx_id = transaction.tx_id if transaction is not None else 1
        roll_ptr = 0
        raw = row.to_bytes(tx_id=tx_id, roll_pointer=roll_ptr)
        if transaction is not None:
            transaction.log_insert_undo(key, raw)
        self._tree.insert(key, raw)
        self._total_rows += 1

    def delete_row(
        self,
        key: Any,
        transaction: Optional[Any] = None,
    ) -> None:
        """
        English: Delete row by primary key; optionally with transaction for Undo Log.
        Chinese: 按主键删除行；可选传入事务以记录 Undo。
        Japanese: 主キーで行を削除；オプションでトランザクションを渡し Undo を記録。
        """
        raw = self._tree.search(key)
        if raw is None:
            raise KeyError(f"Primary key {key} not found")
        if transaction is not None:
            transaction.log_delete_undo(key, raw)
        self._tree.delete(key)
        self._total_rows = max(0, self._total_rows - 1)

    def choose_strategy(
        self,
        start_key: Any,
        end_key: Any,
    ) -> str:
        """
        English: CBO-Lite: choose TABLE_SCAN if range > 30% of table, else INDEX_SCAN.
        Chinese: 代价优化器：若扫描范围超过 30% 则 TABLE_SCAN，否则 INDEX_SCAN。
        Japanese: CBO-Lite：スキャン範囲が 30% 超なら TABLE_SCAN、否則 INDEX_SCAN。

        Returns:
            "TABLE_SCAN" or "INDEX_SCAN"
        """
        if self._total_rows <= 0:
            return "INDEX_SCAN"
        if isinstance(start_key, int) and isinstance(end_key, int):
            scan_range = max(0, end_key - start_key + 1)
        else:
            scan_range = self._total_rows
        if scan_range / self._total_rows > 0.3:
            return "TABLE_SCAN"
        return "INDEX_SCAN"

    def refresh_stats(self) -> None:
        """
        English: Recompute total_rows from range scan (e.g. after load).
        Chinese: 从范围扫描重新计算 total_rows（如加载后）。
        Japanese: 範囲スキャンから total_rows を再計算（ロード後など）。
        """
        self._total_rows = sum(
            1 for _ in self._tree.range_scan(-(2**63), 2**63 - 1)
        )

    def scan_with_condition(
        self,
        condition: Callable[[Tuple], bool],
        start_key: Optional[Any] = None,
        end_key: Optional[Any] = None,
        read_view: Optional[Any] = None,
    ) -> Iterator[Tuple]:
        """
        English: Scan rows in key range; CBO chooses TABLE_SCAN vs INDEX_SCAN; optionally filter by ReadView.
        Chinese: 扫描键范围内行；CBO 自动选择 TABLE_SCAN/INDEX_SCAN；可选按 ReadView 过滤。
        Japanese: キー範囲内の行をスキャン；CBO で TABLE_SCAN/INDEX_SCAN を自動選択；ReadView で可視性フィルタ。

        Args:
            read_view: If provided, only rows visible to this ReadView are yielded.
        """
        lo: Any = start_key if start_key is not None else -(2**63)
        hi: Any = end_key if end_key is not None else (2**63 - 1)
        strategy = self.choose_strategy(lo, hi)
        scan_lo, scan_hi = lo, hi
        if strategy == "TABLE_SCAN":
            scan_lo, scan_hi = -(2**63), 2**63 - 1

        def key_in_range(r: Tuple) -> bool:
            """TABLE_SCAN 时需按用户范围过滤；INDEX_SCAN 时 range_scan 已限制，恒真。"""
            if strategy == "INDEX_SCAN":
                return True
            pk_val = r._values[self._pk_idx]
            return bool(pk_val >= lo and pk_val <= hi)

        def combined_condition(r: Tuple) -> bool:
            if not key_in_range(r):
                return False
            return bool(condition(r))

        for _key, raw in self._tree.range_scan(scan_lo, scan_hi):
            row = Tuple(self._schema, raw=raw)
            if read_view is not None and not read_view.is_visible(row.tx_id):
                continue
            if combined_condition(row):
                yield row

    def rollback_transaction(self, transaction: Any) -> None:
        """
        English: Physical rollback of transaction's modifications on this table.
        Chinese: 物理回滚该事务在本表上的修改。
        Japanese: 本テーブルに対するトランザクションの変更を物理ロールバック。
        """
        transaction.rollback(self._tree)
