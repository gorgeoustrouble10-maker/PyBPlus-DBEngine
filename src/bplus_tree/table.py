"""
表与行存储：Tuple、insert_row、scan_with_condition、TableStats、CBO 代价模型。

English: Table and row storage; Tuple, insert_row, scan with filter, TableStats, CBO cost model.
Chinese: 表与行存储：元组、按行插入、条件扫描、表统计、CBO 代价模型。
Japanese: テーブルと行ストレージ：タプル、行挿入、条件付きスキャン、TableStats、CBO コストモデル。
"""

import struct
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Optional

from bplus_tree.bloom_filter import BloomFilter
from bplus_tree.schema import Schema
from bplus_tree.storage_engine import BPlusTreeEngine, StorageEngine
from bplus_tree.tree import BPlusTree


@dataclass
class TableStats:
    """
    English: Table statistics for CBO; total_rows and index cardinality.
    Chinese: 表统计信息；总行数与索引唯一值分布。
    Japanese: CBO 用テーブル統計；総行数とインデックス基数。
    """
    total_rows: int = 0
    index_cardinality: int = 0  # 主键唯一值数，通常等于 total_rows


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
        engine: Optional[StorageEngine] = None,
        enable_bloom_filter: bool = True,
    ) -> None:
        """
        English: Create table with schema and primary key field.
        Chinese: 用模式与主键字段创建表。

        Args:
            schema: Row schema.
            primary_key: Field name used as B+ tree key (must be INT or comparable).
            tree: Optional existing BPlusTree; if None, creates new one.
            order: B+ tree order when creating new tree.
            tx_manager: Optional TransactionManager for MVCC.
            engine: Optional StorageEngine; if None, wraps tree in BPlusTreeEngine.
            enable_bloom_filter: If True, use Bloom filter to skip IO for absent keys.
        """
        self._schema = schema
        self._pk = primary_key
        if primary_key not in schema.field_names():
            raise ValueError(f"Primary key '{primary_key}' not in schema")
        self._pk_idx = schema.field_names().index(primary_key)
        if engine is not None:
            self._engine = engine
            self._tree = getattr(engine, "tree", engine)
        else:
            bt = tree if tree is not None else BPlusTree(order=order)
            self._engine = BPlusTreeEngine(bt)
            self._tree = bt
        self._tx_manager = tx_manager
        self._total_rows: int = 0
        self._index_unique_counts: dict[str, int] = {}
        self._stats = TableStats()
        self._bloom_filter: Optional[BloomFilter] = (
            BloomFilter(num_bits=16384, num_hashes=4) if enable_bloom_filter else None
        )

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
            transaction.register_table(self)
            transaction.log_insert_undo(key, raw, table=self)
        self._engine.insert(key, raw)
        if self._bloom_filter is not None:
            self._bloom_filter.add(key)
        self._total_rows += 1
        self._stats.total_rows = self._total_rows
        self._stats.index_cardinality = self._total_rows

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
        raw = self._point_lookup(key)
        if raw is None:
            raise KeyError(f"Primary key {key} not found")
        if transaction is not None:
            transaction.register_table(self)
            transaction.log_delete_undo(key, raw, table=self)
        self._engine.delete(key)
        self._total_rows = max(0, self._total_rows - 1)
        self._stats.total_rows = self._total_rows
        self._stats.index_cardinality = self._total_rows

    def compute_cost_table_scan(self) -> float:
        """Cost_TableScan = total_rows * 1.0"""
        return float(self._total_rows) * 1.0

    def compute_cost_index_scan(
        self,
        estimated_range_rows: int,
        index_seek_cost: float = 10.0,
    ) -> float:
        """Cost_IndexScan = estimated_range_rows * 0.5 + index_seek_cost"""
        return float(estimated_range_rows) * 0.5 + index_seek_cost

    def choose_strategy(
        self,
        start_key: Any,
        end_key: Any,
    ) -> str:
        """
        English: CBO with cost model; force TABLE_SCAN if IndexScan cost > TableScan cost.
        Chinese: 基于代价模型；若 IndexScan 代价高于 TableScan 则强制 TABLE_SCAN。
        Japanese: コストモデルベース；IndexScan コスト > TableScan なら TABLE_SCAN を強制。

        Returns:
            "TABLE_SCAN" or "INDEX_SCAN"
        """
        if self._total_rows <= 0:
            return "INDEX_SCAN"
        cost_table = self.compute_cost_table_scan()
        if isinstance(start_key, int) and isinstance(end_key, int):
            estimated_range = max(0, end_key - start_key + 1)
        else:
            estimated_range = self._total_rows
        cost_index = self.compute_cost_index_scan(estimated_range)
        if cost_index > cost_table:
            return "TABLE_SCAN"
        if estimated_range / self._total_rows > 0.3:
            return "TABLE_SCAN"
        return "INDEX_SCAN"

    def _point_lookup(self, key: Any) -> Optional[Any]:
        """
        Point lookup with Bloom filter; skip engine search if filter says absent.
        """
        if self._bloom_filter is not None and not self._bloom_filter.may_contain(key):
            return None
        return self._engine.search(key)

    def apply_insert(self, key: Any, value: Any) -> None:
        """Insert key-value (for WAL replay/replication); updates Bloom filter."""
        self._engine.insert(key, value)
        if self._bloom_filter is not None:
            self._bloom_filter.add(key)
        self._total_rows += 1
        self._stats.total_rows = self._total_rows
        self._stats.index_cardinality = self._total_rows

    def apply_delete(self, key: Any) -> None:
        """Delete by key (for WAL replay/replication)."""
        self._engine.delete(key)
        self._total_rows = max(0, self._total_rows - 1)
        self._stats.total_rows = self._total_rows
        self._stats.index_cardinality = self._total_rows

    def refresh_stats(self) -> None:
        """
        English: Recompute total_rows and index_cardinality from range scan (e.g. after load).
        Chinese: 从范围扫描重新计算 total_rows 与 index_cardinality（如加载后）。
        """
        self._total_rows = 0
        if self._bloom_filter is not None:
            self._bloom_filter = BloomFilter(num_bits=16384, num_hashes=4)
        for k, _ in self._engine.range_scan(-(2**63), 2**63 - 1):
            self._total_rows += 1
            if self._bloom_filter is not None:
                self._bloom_filter.add(k)
        self._stats.total_rows = self._total_rows
        self._stats.index_cardinality = self._total_rows

    def scan_with_condition(
        self,
        condition: Callable[[Tuple], bool],
        start_key: Optional[Any] = None,
        end_key: Optional[Any] = None,
        in_values: Optional[list[Any]] = None,
        read_view: Optional[Any] = None,
    ) -> Iterator[Tuple]:
        """
        English: Scan rows in key range or IN values; CBO chooses TABLE_SCAN vs INDEX_SCAN; optionally filter by ReadView.
        Chinese: 扫描键范围内或 IN 多值行；CBO 自动选择 TABLE_SCAN/INDEX_SCAN；可选按 ReadView 过滤。
        Japanese: キー範囲または IN 値でスキャン；CBO で TABLE_SCAN/INDEX_SCAN を自動選択；ReadView で可視性フィルタ。

        Args:
            in_values: If provided, do point lookups for each value (WHERE col IN (v1,v2)).
            read_view: If provided, only rows visible to this ReadView are yielded.
        """
        if in_values is not None and in_values:
            for k in in_values:
                raw = self._point_lookup(k)
                if raw is None:
                    continue
                row = Tuple(self._schema, raw=raw)
                if read_view is not None and not read_view.is_visible(row.tx_id):
                    continue
                if condition(row):
                    yield row
            return

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

        for _key, raw in self._engine.range_scan(scan_lo, scan_hi):
            row = Tuple(self._schema, raw=raw)
            if read_view is not None and not read_view.is_visible(row.tx_id):
                continue
            if combined_condition(row):
                yield row

    def rollback_transaction(self, transaction: Any) -> None:
        """
        English: Physical rollback of transaction's modifications (all involved tables).
        Chinese: 物理回滚该事务的修改（所有涉及表，多表原子回滚）。
        Japanese: トランザクションの変更を物理ロールバック（全関与テーブル、マルチテーブル原子）。
        """
        transaction.rollback()
