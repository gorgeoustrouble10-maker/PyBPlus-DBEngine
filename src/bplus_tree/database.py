"""
持久化表与元数据管理：Superblock、二级索引、open/close。

English: Persistent table with superblock, secondary indexes, open/close.
Chinese: 持久化表：元数据块、二级索引、打开/关闭。
Japanese: 永続化テーブル：スーパーブロック、セカンダリインデックス、オープン/クローズ。
"""

import base64
import json
import struct
from pathlib import Path
from typing import Any, Callable, Iterator, Optional

from bplus_tree.schema import Schema
from bplus_tree.table import RowTable, Tuple
from bplus_tree.tree import BPlusTree

# Page 0 大小，用于 Superblock
PAGE_SIZE: int = 4096
SUPERBLOCK_MAGIC: bytes = b"DBT1"


def _schema_to_dict(schema: Schema) -> dict[str, Any]:
    """将 Schema 序列化为可 JSON 存取的字典。"""
    return {
        "fields": list(schema._fields),
        "varchar_lengths": dict(schema._varchar_lengths),
    }


def _pack_superblock(sb: dict[str, Any]) -> bytes:
    """将 superblock 打包为 Page 0（4096 字节）。"""
    pk = sb["primary_key"].encode("utf-8")
    schema_json = json.dumps(sb["schema"]).encode("utf-8")
    order = int(sb.get("order", 16))
    primary_root = int(
        sb.get("_primary_root", 0)
    )  # 由 flush 传入
    sec_items = list(sb.get("_secondary_roots", {}).items())

    chunks: list[bytes] = []
    chunks.append(SUPERBLOCK_MAGIC)
    chunks.append(struct.pack("<H", len(pk)))
    chunks.append(pk)
    chunks.append(struct.pack("<I", len(schema_json)))
    chunks.append(schema_json)
    chunks.append(struct.pack("<I", order))
    chunks.append(struct.pack("<i", primary_root))
    chunks.append(struct.pack("<H", len(sec_items)))
    for field, rid in sec_items:
        fb = field.encode("utf-8")
        chunks.append(struct.pack("<H", len(fb)))
        chunks.append(fb)
        chunks.append(struct.pack("<i", rid))
    out = b"".join(chunks)
    return out.ljust(PAGE_SIZE, b"\x00")


def _unpack_superblock(raw: bytes) -> dict[str, Any]:
    """从 Page 0 解包 superblock。"""
    if len(raw) < 4 or raw[:4] != SUPERBLOCK_MAGIC:
        return {}
    off = 4
    pk_len = struct.unpack_from("<H", raw, off)[0]
    off += 2
    pk = raw[off : off + pk_len].decode("utf-8")
    off += pk_len
    schema_len = struct.unpack_from("<I", raw, off)[0]
    off += 4
    schema_json = raw[off : off + schema_len].decode("utf-8")
    off += schema_len
    schema = json.loads(schema_json)
    order = struct.unpack_from("<I", raw, off)[0]
    off += 4
    primary_root = struct.unpack_from("<i", raw, off)[0]
    off += 4
    num_sec = struct.unpack_from("<H", raw, off)[0]
    off += 2
    sec_roots: dict[str, int] = {}
    for _ in range(num_sec):
        flen = struct.unpack_from("<H", raw, off)[0]
        off += 2
        field = raw[off : off + flen].decode("utf-8")
        off += flen
        rid = struct.unpack_from("<i", raw, off)[0]
        off += 4
        sec_roots[field] = rid
    return {
        "schema": schema,
        "primary_key": pk,
        "order": order,
        "_primary_root": primary_root,
        "_secondary_roots": sec_roots,
    }


def _schema_from_dict(d: dict[str, Any]) -> Schema:
    """从字典反序列化 Schema。"""
    raw_fields = d["fields"]
    fields = [tuple(f) for f in raw_fields]
    schema = Schema(
        fields=fields,
        varchar_lengths=dict(d.get("varchar_lengths", {})),
    )
    return schema


def _serialize_tree_for_db(
    root: Any, order: int
) -> tuple[list[dict[str, Any]], int]:
    """序列化树为 JSON 可存储格式；叶值 bytes 转 base64。"""
    if root is None:
        return [], -1
    from bplus_tree.node import BPlusTreeNode, InternalNode, LeafNode

    all_nodes: list[Any] = []
    seen: set[int] = set()
    q: list[Any] = [root]
    while q:
        n = q.pop(0)
        if id(n) in seen:
            continue
        seen.add(id(n))
        all_nodes.append(n)
        if n.is_leaf and isinstance(n, LeafNode):
            nxt = getattr(n, "next", None)
            if isinstance(nxt, LeafNode) and id(nxt) not in seen:
                q.append(nxt)
        elif isinstance(n, InternalNode):
            for c in n.children:
                if isinstance(c, BPlusTreeNode) and id(c) not in seen:
                    q.append(c)

    node_to_id = {id(n): i for i, n in enumerate(all_nodes)}
    leaves: list[LeafNode] = []
    node = root
    while not node.is_leaf and isinstance(node, InternalNode):
        node = node.children[0]
    cur = node if isinstance(node, LeafNode) else None
    while cur is not None:
        leaves.append(cur)
        nxt = getattr(cur, "next", None)
        cur = nxt if isinstance(nxt, LeafNode) else None
    leaf_ids = [node_to_id[id(l)] for l in leaves]
    nodes_data: list[dict[str, Any]] = []
    leaf_pos = {lid: i for i, lid in enumerate(leaf_ids)}
    for i, n in enumerate(all_nodes):
        if n.is_leaf and isinstance(n, LeafNode):
            idx = leaf_pos.get(i, 0)
            prev_id = leaf_ids[idx - 1] if idx > 0 else -1
            next_id = leaf_ids[idx + 1] if idx + 1 < len(leaf_ids) else -1
            vals = [
                base64.b64encode(v).decode("ascii") if isinstance(v, bytes) else v
                for v in n.values
            ]
            nodes_data.append(
                {
                    "id": i,
                    "is_leaf": True,
                    "keys": n.keys,
                    "values": vals,
                    "prev_id": prev_id,
                    "next_id": next_id,
                }
            )
        else:
            if isinstance(n, InternalNode):
                children_ids = [node_to_id[id(c)] for c in n.children]
                nodes_data.append(
                    {
                        "id": i,
                        "is_leaf": False,
                        "keys": n.keys,
                        "children_ids": children_ids,
                    }
                )
    root_id = node_to_id[id(root)] if nodes_data else -1
    return nodes_data, root_id


def _deserialize_tree_from_db(
    nodes_data: list[dict[str, Any]], root_id: int, order: int
) -> BPlusTree:
    """从 JSON 结构反序列化出 BPlusTree；base64 还原为 bytes。"""
    from bplus_tree.node import InternalNode, LeafNode

    def _maybe_tuple(k: Any) -> Any:
        """JSON 将 tuple 反序列化为 list，还原为 tuple 以支持复合键比较。"""
        if isinstance(k, list) and len(k) == 2:
            return (k[0], k[1])
        return k

    nodes: list[Any] = []
    for nd in nodes_data:
        if nd.get("is_leaf"):
            ln = LeafNode()
            ln.keys = [_maybe_tuple(k) for k in nd["keys"]]
            raw_vals = list(nd["values"])
            vals: list[Any] = []
            for v in raw_vals:
                if isinstance(v, str):
                    try:
                        vals.append(base64.b64decode(v.encode("ascii")))
                    except Exception:
                        vals.append(v)
                else:
                    vals.append(v)
            ln.values = vals
            nodes.append(ln)
        else:
            internal = InternalNode()
            internal.keys = [_maybe_tuple(k) for k in nd["keys"]]
            nodes.append(internal)
    for nd, node in zip(nodes_data, nodes):
        if not nd.get("is_leaf") and isinstance(node, InternalNode):
            node.children = [nodes[cid] for cid in nd["children_ids"]]
    for nd, node in zip(nodes_data, nodes):
        if nd.get("is_leaf") and isinstance(node, LeafNode):
            prev_id = nd.get("prev_id", -1)
            next_id = nd.get("next_id", -1)
            node.prev = nodes[prev_id] if prev_id >= 0 else None
            node.next = nodes[next_id] if next_id >= 0 else None
    tree = BPlusTree(order=order)
    tree._root = nodes[root_id] if root_id >= 0 and nodes else None
    return tree


class PersistentTable:
    """
    English: Persistent table with superblock (Page 0), schema, primary index, secondary indexes.
    Chinese: 持久化表：Superblock（Page 0）、Schema、主索引与二级索引。
    Japanese: 永続化テーブル：Superblock（Page 0）、スキーマ、主索引とセカンダリインデックス。
    """

    def __init__(
        self,
        schema: Schema,
        primary_key: str,
        filepath: Optional[Path] = None,
        order: int = 16,
    ) -> None:
        """
        English: Create persistent table; use open() to load from file.
        Chinese: 创建持久化表；使用 open() 从文件加载。
        Japanese: 永続化テーブルを作成；open() でファイルからロードします。

        Args:
            schema: Row schema.
            primary_key: Primary key field name.
            filepath: Optional backing file path.
            order: B+ tree order.
        """
        self._schema = schema
        self._pk = primary_key
        self._pk_idx = schema.field_names().index(primary_key)
        self._filepath: Optional[Path] = Path(filepath) if filepath else None
        self._order = order
        self._table = RowTable(schema, primary_key, order=order)
        self._indexes: dict[str, BPlusTree] = {}  # field_name -> tree

    @classmethod
    def create(cls, schema: Schema, primary_key: str, filepath: str | Path) -> "PersistentTable":
        """
        English: Create new persistent table and save empty superblock.
        Chinese: 创建新的持久化表并写入空 Superblock。
        Japanese: 新規永続化テーブルを作成し、空の Superblock を保存します。
        """
        if primary_key not in schema.field_names():
            raise ValueError(f"Primary key '{primary_key}' not in schema")
        tbl = cls(schema, primary_key, filepath=Path(filepath))
        tbl.flush()
        return tbl

    @classmethod
    def open(cls, filepath: str | Path) -> "PersistentTable":
        """
        English: Open table from .db file; load schema and indexes automatically.
        Chinese: 从 .db 文件打开表；自动加载 Schema 与索引。
        Japanese: .db ファイルからテーブルを開く；スキーマとインデックスを自動ロード。

        Args:
            filepath: Path to database file (.db or .json).

        Returns:
            Loaded PersistentTable instance.
        """
        path = Path(filepath)
        if not path.exists():
            raise FileNotFoundError(f"Database file not found: {path}")

        with path.open("rb") as f:
            head = f.read(4)
        if head == SUPERBLOCK_MAGIC:
            with path.open("rb") as f:
                page0 = f.read(PAGE_SIZE)
                json_part = f.read()
            sb = _unpack_superblock(page0)
            if not sb:
                raise ValueError("Invalid superblock in database file")
            schema = _schema_from_dict(sb["schema"])
            pk = str(sb["primary_key"])
            order = int(sb["order"])
            payload = json.loads(json_part.decode("utf-8"))
        else:
            with path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            sb = payload.get("superblock", payload)
            schema = _schema_from_dict(sb["schema"])
            pk = str(sb["primary_key"])
            order = int(sb.get("order", 16))

        tbl = cls(schema, pk, filepath=path, order=order)
        primary_data = payload.get("primary_index", {})
        if primary_data:
            nodes = primary_data.get("nodes", [])
            root_id = int(primary_data.get("root_id", -1))
            tree = _deserialize_tree_from_db(nodes, root_id, order)
            tbl._table._tree = tree

        for field, idx_data in payload.get("secondary_indexes", {}).items():
            nodes = idx_data.get("nodes", [])
            root_id = int(idx_data.get("root_id", -1))
            tree = _deserialize_tree_from_db(nodes, root_id, order)
            tbl._indexes[field] = tree

        return tbl

    def create_index(self, field_name: str) -> None:
        """
        English: Create secondary index on field; key=field value, value=primary key.
        Chinese: 在字段上创建二级索引；key=字段值，value=主键。
        Japanese: フィールドにセカンダリインデックスを作成；key=フィールド値、value=主キー。

        Args:
            field_name: Field to index.
        """
        if field_name not in self._schema.field_names():
            raise ValueError(f"Field '{field_name}' not in schema")
        if field_name in self._indexes:
            return
        idx_tree = BPlusTree(order=self._order)
        self._indexes[field_name] = idx_tree
        field_idx = self._schema.field_names().index(field_name)
        for pk_key, raw in self._table._tree.range_scan(-(2**63), 2**63 - 1):
            row = Tuple(self._schema, raw=raw)
            field_val = row._values[field_idx]
            idx_tree.insert((field_val, pk_key), 1)

    def insert_row(self, tuple_data: list[Any]) -> None:
        """
        English: Insert row; sync primary and all secondary indexes.
        Chinese: 插入行；同步更新主索引与所有二级索引。
        Japanese: 行を挿入；主索引と全セカンダリインデックスを同期更新。
        """
        if len(tuple_data) != len(self._schema):
            raise ValueError(
                f"Row has {len(tuple_data)} values, schema expects {len(self._schema)}"
            )
        pk_val = tuple_data[self._pk_idx]
        row = Tuple(self._schema, values=tuple_data)
        self._table._tree.insert(pk_val, row.to_bytes())
        for field_name, idx_tree in self._indexes.items():
            field_idx = self._schema.field_names().index(field_name)
            field_val = tuple_data[field_idx]
            idx_tree.insert((field_val, pk_val), 1)

    def delete_row(self, pk_value: Any) -> None:
        """
        English: Delete row by primary key; sync secondary indexes.
        Chinese: 按主键删除行；同步更新二级索引。
        Japanese: 主キーで行を削除；セカンダリインデックスを同期更新。
        """
        raw = self._table._tree.search(pk_value)
        if raw is None:
            raise KeyError(f"Primary key {pk_value} not found")
        row = Tuple(self._schema, raw=raw)
        for field_name, idx_tree in self._indexes.items():
            field_val = row.get_field(field_name)
            idx_tree.delete((field_val, pk_value))
        self._table._tree.delete(pk_value)

    def get_by_index(self, field_name: str, value: Any) -> Iterator[Tuple]:
        """
        English: Lookup by secondary index; O(log N) when index exists.
        Chinese: 按二级索引查询；有索引时 O(log N)。
        Japanese: セカンダリインデックスで検索；インデックスあれば O(log N)。

        Args:
            field_name: Indexed field.
            value: Value to match.

        Yields:
            Matching rows.
        """
        if field_name in self._indexes:
            idx_tree = self._indexes[field_name]
            lo = (value, -(2**63))
            hi = (value, 2**63 - 1)
            for (fv, pk_val), _ in idx_tree.range_scan(lo, hi):
                if fv != value:
                    break
                raw = self._table._tree.search(pk_val)
                if raw is not None:
                    yield Tuple(self._schema, raw=raw)
        else:
            for row in self._table.scan_with_condition(
                lambda r: r.get_field(field_name) == value
            ):
                yield row

    def scan_with_condition(
        self,
        condition: Callable[[Tuple], bool],
        start_key: Optional[Any] = None,
        end_key: Optional[Any] = None,
    ) -> Iterator[Tuple]:
        """
        English: Delegate to underlying RowTable.
        Chinese: 委托给底层 RowTable。
        Japanese: 下層の RowTable に委譲します。
        """
        it: Iterator[Tuple] = self._table.scan_with_condition(
            condition, start_key, end_key
        )
        return it

    def flush(self) -> None:
        """
        English: Persist schema and all indexes to file.
        Chinese: 将 Schema 与所有索引持久化到文件。
        Japanese: スキーマと全インデックスをファイルに永続化。
        """
        if not self._filepath:
            return
        self._filepath.parent.mkdir(parents=True, exist_ok=True)
        primary_nodes, root_id = _serialize_tree_for_db(
            self._table._tree._root, self._order
        )
        primary_payload: dict[str, Any] = {
            "nodes": primary_nodes,
            "root_id": root_id,
        }
        secondary_payload: dict[str, Any] = {}
        for field, idx_tree in self._indexes.items():
            idx_nodes, idx_root = _serialize_tree_for_db(idx_tree._root, self._order)
            secondary_payload[field] = {"nodes": idx_nodes, "root_id": idx_root}

        sec_roots = {
            f: _serialize_tree_for_db(t._root, self._order)[1]
            for f, t in self._indexes.items()
        }
        superblock = {
            "schema": _schema_to_dict(self._schema),
            "primary_key": self._pk,
            "order": self._order,
            "_primary_root": root_id,
            "_secondary_roots": sec_roots,
        }
        payload = {
            "superblock": {
                "schema": superblock["schema"],
                "primary_key": superblock["primary_key"],
                "order": superblock["order"],
            },
            "primary_index": primary_payload,
            "secondary_indexes": secondary_payload,
        }
        json_bytes = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        sb = _pack_superblock(superblock)
        with self._filepath.open("wb") as f:
            f.write(sb)
            f.write(json_bytes)
