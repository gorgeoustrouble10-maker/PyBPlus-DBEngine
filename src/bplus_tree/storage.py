"""
二进制页管理与 Buffer Pool。

English: Binary page layout and buffer pool for disk-backed B+ tree storage.
Chinese: 二进制页格式及 Buffer Pool，用于基于磁盘的 B+ 树存储。
Japanese: ディスクベース B+ 木ストレージのためのバイナリページレイアウトとバッファプール。
"""

import os
import struct
from collections import OrderedDict
from pathlib import Path
from typing import Any, Optional

from bplus_tree.node import InternalNode, LeafNode

# 页大小 4096 字节
PAGE_SIZE: int = 4096
# Buffer Pool 最大缓存页数
MAX_PAGES: int = 64

# 页类型枚举
PAGE_TYPE_INTERNAL: int = 0
PAGE_TYPE_LEAF: int = 1  # 旧版 leaf（向后兼容）
PAGE_TYPE_LEAF_SLOTTED: int = 2  # Phase 12: 槽位页格式

# 旧版 Header 布局：type(1) + key_count(2) + parent_id(4) = 7 bytes
HEADER_SIZE: int = 16
# Phase 12: Slotted Leaf Header 固定 24 字节
LEAF_SLOTTED_HEADER_SIZE: int = 24
SLOTTED_SLOT_SIZE: int = 4  # 每 slot: offset(2) + length(2)
# parent_id 为 -1 表示无父（根）
INVALID_PAGE_ID: int = -1


class FreeSpaceMap:
    """
    English: Bitmap tracking freed pages; get_new_page prefers reuse over append.
    Chinese: 空闲空间映射：位图追踪已释放页，优先复用而非追加。
    Japanese: 空き領域マップ：ビットマップで解放済みページを追跡、追記より再利用を優先。
    """

    def __init__(self) -> None:
        """
        English: Create FSM; free_ids holds released page IDs.
        Chinese: 创建 FSM；free_ids 存储已释放的页 ID。
        Japanese: FSM を作成；free_ids に解放済みページ ID を保持。
        """
        self._free_ids: set[int] = set()

    def add_free(self, page_id: int) -> None:
        """
        English: Mark page as free for reuse.
        Chinese: 将页标记为空闲供复用。
        Japanese: ページを空きとしてマークし、再利用可能にします。
        """
        self._free_ids.add(page_id)

    def pop_free(self) -> int | None:
        """
        English: Take one free page ID if any; None if empty.
        Chinese: 若有空闲页则弹出一个；否则返回 None。
        Japanese: 空きページがあれば 1 つ取り出す；なければ None。
        """
        if not self._free_ids:
            return None
        return self._free_ids.pop()

    def free_count(self) -> int:
        """Number of free pages / 空闲页数量。"""
        return len(self._free_ids)


def _ensure_int_key(key: Any) -> int:
    """保证 key 为 int，用于二进制序列化。"""
    if isinstance(key, int):
        return key
    raise TypeError(f"Binary storage requires int keys, got {type(key)}")


def _ensure_str_value(val: Any) -> str:
    """保证 value 为 str（旧接口兼容）。"""
    return str(val)


def _value_to_bytes(val: Any) -> bytes:
    """
    English: Convert value to bytes for storage; support bytes and str.
    Chinese: 将 value 转为存储字节；支持 bytes 与 str。
    Japanese: 値をストレージ用バイトに変換；bytes と str をサポート。
    """
    if isinstance(val, bytes):
        return val
    return str(val).encode("utf-8")


def serialize_internal_page(
    keys: list[int],
    children_ids: list[int],
    parent_id: int = INVALID_PAGE_ID,
) -> bytes:
    """
    English: Serialize internal node to fixed-size page bytes.
    Chinese: 将内部节点序列化为固定大小页字节。
    Japanese: 内部ノードを固定サイズのページバイトにシリアライズします。

    Layout: header(16) + keys(n*8) + child_ids((n+1)*4)
    """
    key_count = len(keys)
    header = struct.pack("<BHi", PAGE_TYPE_INTERNAL, key_count, parent_id)
    keys_b = struct.pack(f"<{key_count}q", *keys) if key_count else b""
    children_b = struct.pack(f"<{key_count + 1}i", *children_ids)
    return (header.ljust(HEADER_SIZE, b"\x00") + keys_b + children_b).ljust(
        PAGE_SIZE, b"\x00"
    )


def serialize_leaf_page(
    keys: list[int],
    values: list[str],
    prev_id: int = INVALID_PAGE_ID,
    next_id: int = INVALID_PAGE_ID,
    parent_id: int = INVALID_PAGE_ID,
) -> bytes:
    """
    English: Serialize leaf node to fixed-size page bytes (legacy format).
    Chinese: 将叶子节点序列化为固定大小页字节（旧版格式）。
    Japanese: 葉ノードを固定サイズのページバイトにシリアライズ（旧形式）。
    """
    key_count = len(keys)
    key_ints = [_ensure_int_key(k) for k in keys]
    val_strs = [_ensure_str_value(v) for v in values]

    header = struct.pack("<BHi", PAGE_TYPE_LEAF, key_count, parent_id)
    keys_bytes = struct.pack(f"<{key_count}q", *key_ints) if key_count else b""
    link_bytes = struct.pack("<ii", prev_id, next_id)

    vals_data = b""
    for v in val_strs:
        vb = v.encode("utf-8")
        vals_data += struct.pack("<H", len(vb)) + vb

    body = keys_bytes + link_bytes + vals_data
    total = header.ljust(HEADER_SIZE, b"\x00") + body
    return total.ljust(PAGE_SIZE, b"\x00")


def _pack_slotted_header(
    key_count: int,
    parent_id: int,
    prev_id: int,
    next_id: int,
    slot_array_end: int,
    free_start: int,
) -> bytes:
    """Pack 24-byte slotted leaf header. 按 SLOTTED_PAGE_LAYOUT 逐块 pack，避免格式混乱。"""
    b1 = struct.pack("<B", PAGE_TYPE_LEAF_SLOTTED)
    b2 = struct.pack("<H", key_count)
    b3 = struct.pack("<i", parent_id)
    b4 = struct.pack("<i", prev_id)
    b5 = struct.pack("<i", next_id)
    b6 = struct.pack("<H", slot_array_end)
    b7 = struct.pack("<H", free_start)
    b8 = b"\x00" * 5  # reserved + checksum + padding
    return b1 + b2 + b3 + b4 + b5 + b6 + b7 + b8


def serialize_leaf_page_slotted(
    keys: list[int],
    values: list[str] | list[bytes],
    prev_id: int = INVALID_PAGE_ID,
    next_id: int = INVALID_PAGE_ID,
    parent_id: int = INVALID_PAGE_ID,
) -> bytes:
    """
    English: Serialize leaf to Slotted Page layout; Header|SlotArray|FreeSpace|Records.
    Chinese: 将叶子序列化为槽位页布局；Header | SlotArray | 空闲区 | Records（从页尾向前）。
    Japanese: 葉をスロット付きページレイアウトでシリアライズ；Header|SlotArray|空き|Records。
    """
    key_ints = [_ensure_int_key(k) for k in keys]
    val_bytes_list = [_value_to_bytes(v) for v in values]

    # 1. 构建 records：每条约 (key 8B + value_len 2B + value)
    records: list[tuple[int, bytes]] = []
    for k, vb in zip(key_ints, val_bytes_list):
        rec = struct.pack("<qH", k, len(vb)) + vb
        records.append((k, rec))

    # 2. Records 从 free_start 起连续存放；free_start = PAGE_SIZE - total_records_size
    total_records_len = sum(len(rec) for _k, rec in records)
    free_start = PAGE_SIZE - total_records_len
    if free_start < LEAF_SLOTTED_HEADER_SIZE:
        raise ValueError("Leaf page overflow")

    # 3. 每个 slot 指向对应 record 的 (offset, length)
    running = free_start
    slots: list[tuple[int, int]] = []
    for _k, rec in records:
        rlen = len(rec)
        slots.append((running, rlen))
        running += rlen

    slot_array_end = LEAF_SLOTTED_HEADER_SIZE + len(slots) * SLOTTED_SLOT_SIZE
    if free_start < slot_array_end:
        raise ValueError("Leaf page overflow: free_start < slot_array_end")

    # 3. 组装页：header + slot_array + padding + records
    header = _pack_slotted_header(
        len(keys), parent_id, prev_id, next_id, slot_array_end, free_start
    )
    slot_bytes = b""
    for off, ln in slots:
        slot_bytes += struct.pack("<HH", off, ln)
    free_space_len = free_start - slot_array_end
    body_mid = b"\x00" * free_space_len if free_space_len > 0 else b""
    records_bytes = b""
    for _k, rec in records:
        records_bytes += rec
    total = header + slot_bytes + body_mid + records_bytes
    return total.ljust(PAGE_SIZE, b"\x00")


def _compact_slotted_page(raw: bytes) -> bytes:
    """
    English: Compact slotted leaf page; reclaim fragmentation from deletes.
    Chinese: 整理槽位页；回收删除造成的碎片。
    Japanese: スロット付き葉ページをコンパクト；削除による断片化を解消。
    """
    if len(raw) < LEAF_SLOTTED_HEADER_SIZE or raw[0] != PAGE_TYPE_LEAF_SLOTTED:
        return raw
    key_count = struct.unpack_from("<H", raw, 1)[0]
    parent_id = struct.unpack_from("<i", raw, 3)[0]
    prev_id = struct.unpack_from("<i", raw, 7)[0]
    next_id = struct.unpack_from("<i", raw, 11)[0]

    # 收集有效 slot：(offset, length) -> record_bytes
    INVALID_OFF: int = 0xFFFF
    valid: list[bytes] = []
    for i in range(key_count):
        so = LEAF_SLOTTED_HEADER_SIZE + i * SLOTTED_SLOT_SIZE
        off, ln = struct.unpack_from("<HH", raw, so)
        if off == INVALID_OFF and ln == 0:
            continue
        valid.append(raw[off : off + ln])

    if not valid:
        return _pack_slotted_header(0, parent_id, prev_id, next_id, LEAF_SLOTTED_HEADER_SIZE, PAGE_SIZE) + b"\x00" * (PAGE_SIZE - LEAF_SLOTTED_HEADER_SIZE)

    # 从页尾向前重写
    free_start = PAGE_SIZE
    new_slots: list[tuple[int, int]] = []
    for rec in valid:
        free_start -= len(rec)
        new_slots.append((free_start, len(rec)))
    slot_array_end = LEAF_SLOTTED_HEADER_SIZE + len(new_slots) * SLOTTED_SLOT_SIZE

    header = _pack_slotted_header(
        len(new_slots), parent_id, prev_id, next_id, slot_array_end, free_start
    )
    slot_bytes = b"".join(struct.pack("<HH", o, l) for o, l in new_slots)
    free_space = b"\x00" * (free_start - slot_array_end)
    records_bytes = b"".join(valid)
    return (header + slot_bytes + free_space + records_bytes).ljust(PAGE_SIZE, b"\x00")


def _deserialize_leaf_slotted(raw: bytes) -> LeafNode:
    """Deserialize slotted leaf page (PAGE_TYPE_LEAF_SLOTTED)."""
    if len(raw) < LEAF_SLOTTED_HEADER_SIZE:
        raise ValueError("Slotted leaf page too small")
    key_count = struct.unpack_from("<H", raw, 1)[0]
    parent_id = struct.unpack_from("<i", raw, 3)[0]
    prev_id = struct.unpack_from("<i", raw, 7)[0]
    next_id = struct.unpack_from("<i", raw, 11)[0]
    slot_array_end = struct.unpack_from("<H", raw, 15)[0]

    keys: list[int] = []
    values: list[str] = []
    INVALID_OFF: int = 0xFFFF
    for i in range(key_count):
        so = LEAF_SLOTTED_HEADER_SIZE + i * SLOTTED_SLOT_SIZE
        if so + 4 > len(raw):
            break
        rec_off, rec_len = struct.unpack_from("<HH", raw, so)
        if rec_off == INVALID_OFF and rec_len == 0:
            continue
        if rec_off + rec_len > len(raw) or rec_len < 10:
            continue
        k = struct.unpack_from("<q", raw, rec_off)[0]
        vlen = struct.unpack_from("<H", raw, rec_off + 8)[0]
        if vlen > rec_len - 10:
            continue
        vb = raw[rec_off + 10 : rec_off + 10 + vlen]
        keys.append(k)
        values.append(vb.decode("utf-8"))
    node = LeafNode()
    node.keys = keys
    node.values = values
    node.prev_id = prev_id
    node.next_id = next_id
    return node


def deserialize_page(raw: bytes, page_id: int) -> InternalNode | LeafNode:
    """
    English: Deserialize page bytes into InternalNode or LeafNode.
    Chinese: 将页字节反序列化为 InternalNode 或 LeafNode。
    Japanese: ページバイトを InternalNode または LeafNode にデシリアライズします。
    """
    if len(raw) < HEADER_SIZE:
        raise ValueError("Page too small")
    page_type = raw[0]
    key_count = struct.unpack_from("<H", raw, 1)[0]
    parent_id = struct.unpack_from("<i", raw, 3)[0]
    off = HEADER_SIZE

    if page_type == PAGE_TYPE_INTERNAL:
        keys = list(struct.unpack_from(f"<{key_count}q", raw, off))
        off += key_count * 8
        children_ids = list(struct.unpack_from(f"<{key_count + 1}i", raw, off))
        node = InternalNode()
        node.keys = keys
        node.children = children_ids
        return node
    elif page_type == PAGE_TYPE_LEAF_SLOTTED:
        return _deserialize_leaf_slotted(raw)
    else:
        # Legacy leaf
        keys = list(struct.unpack_from(f"<{key_count}q", raw, off))
        off += key_count * 8
        prev_id, next_id = struct.unpack_from("<ii", raw, off)
        off += 8
        values = []
        for _ in range(key_count):
            vlen = struct.unpack_from("<H", raw, off)[0]
            off += 2
            values.append(raw[off : off + vlen].decode("utf-8"))
            off += vlen
        node = LeafNode()
        node.keys = keys
        node.values = values
        node.prev_id = prev_id
        node.next_id = next_id
        return node


class BufferPool:
    """
    English: LRU buffer pool with FSM; allocate_page prefers free pages.
    Chinese: 带 FSM 的 LRU 缓冲池；allocate_page 优先复用空闲页。
    Japanese: FSM 付き LRU バッファプール；allocate_page は空きページを優先。
    """

    def __init__(
        self,
        filepath: Optional[Path] = None,
        max_pages: int = MAX_PAGES,
    ) -> None:
        """
        English: Create buffer pool; filepath=None for in-memory only.
        Chinese: 创建缓冲池；filepath=None 时仅内存模式。
        Japanese: バッファプールを作成；filepath=None の場合はメモリのみ。
        """
        self._filepath: Optional[Path] = filepath
        self._max_pages: int = max_pages
        self._cache: OrderedDict[int, tuple[bytes, bool]] = OrderedDict()
        self._next_page_id: int = 1
        self._root_page_id: int = INVALID_PAGE_ID
        self._fsm: FreeSpaceMap = FreeSpaceMap()
        if filepath and filepath.exists() and filepath.stat().st_size >= 16:
            self._load_header()

    def _load_header(self) -> None:
        """从文件头加载 root_page_id 和 next_page_id。"""
        if not self._filepath:
            return
        with open(self._filepath, "rb") as f:
            magic = f.read(4)
            if magic != b"B+DB":
                return
            self._root_page_id = struct.unpack("<i", f.read(4))[0]
            self._next_page_id = struct.unpack("<i", f.read(4))[0]

    def _save_header(self) -> None:
        """将 root_page_id 和 next_page_id 写入文件头。"""
        if not self._filepath:
            return
        self._filepath.parent.mkdir(parents=True, exist_ok=True)
        mode = "r+b" if self._filepath.exists() else "w+b"
        with open(self._filepath, mode) as f:
            f.seek(0)
            f.write(b"B+DB")
            f.write(struct.pack("<i", self._root_page_id))
            f.write(struct.pack("<i", self._next_page_id))

    def _read_page_from_disk(self, page_id: int) -> bytes:
        """从磁盘读取一页。"""
        if not self._filepath or not self._filepath.exists():
            raise FileNotFoundError("No backing file")
        with open(self._filepath, "rb") as f:
            f.seek(16 + page_id * PAGE_SIZE)  # 跳过 16 字节头
            return f.read(PAGE_SIZE)

    def _write_page_to_disk(self, page_id: int, data: bytes) -> None:
        """将一页写入磁盘；必要时扩展文件大小。"""
        if not self._filepath:
            return
        offset = 16 + page_id * PAGE_SIZE
        required = offset + PAGE_SIZE
        with open(self._filepath, "r+b") as f:
            f.seek(0, 2)
            if f.tell() < required:
                f.seek(required - 1)
                f.write(b"\x00")
            f.seek(offset)
            f.write(data)

    def get_root_page_id(self) -> int:
        """返回根页 ID。"""
        return self._root_page_id

    def set_root_page_id(self, page_id: int) -> None:
        """设置根页 ID。"""
        self._root_page_id = page_id
        if self._filepath:
            self._save_header()

    def allocate_page(self) -> int:
        """
        English: Allocate page ID; prefer FSM reuse over append.
        Chinese: 分配页 ID；优先从 FSM 复用空闲页，否则追加。
        Japanese: ページ ID を割り当て；FSM の再利用を優先、否則は追記。
        """
        pid = self._fsm.pop_free()
        if pid is not None:
            return pid
        pid = self._next_page_id
        self._next_page_id += 1
        if self._filepath:
            self._save_header()
        return pid

    def free_page(self, page_id: int) -> None:
        """
        English: Return page to FSM for reuse.
        Chinese: 将页归还 FSM 供复用。
        Japanese: ページを FSM に返却し、再利用可能にします。
        """
        self._cache.pop(page_id, None)
        self._fsm.add_free(page_id)

    get_new_page = allocate_page

    def get_page(self, page_id: int) -> InternalNode | LeafNode:
        """
        English: Get page by ID; load from disk if not cached.
        Chinese: 根据 ID 获取页；若未缓存则从磁盘读取。
        Japanese: ID でページ取得；キャッシュになければディスクから読込。
        """
        if page_id in self._cache:
            # 1. 命中缓存，移到末尾（LRU 最近使用）
            raw, _ = self._cache.pop(page_id)
            self._cache[page_id] = (raw, False)
            return deserialize_page(raw, page_id)

        # 2. 未命中：从磁盘读取
        if self._filepath and self._filepath.exists():
            raw = self._read_page_from_disk(page_id)
        else:
            raise KeyError(f"Page {page_id} not in cache and no file")

        # 3. 放入缓存，可能触发 LRU 淘汰
        self._put_in_cache(page_id, raw, dirty=False)
        return deserialize_page(raw, page_id)

    def _put_in_cache(self, page_id: int, data: bytes, dirty: bool) -> None:
        """将页放入缓存，满时 LRU 淘汰。"""
        while len(self._cache) >= self._max_pages and self._cache:
            # 4. 淘汰最久未用的页（OrderedDict 首项）
            evict_id, (evict_raw, evict_dirty) = self._cache.popitem(last=False)
            if evict_dirty and self._filepath:
                self._write_page_to_disk(evict_id, evict_raw)
        self._cache[page_id] = (data, dirty)

    def put_page(
        self,
        page_id: int,
        node: InternalNode | LeafNode,
    ) -> None:
        """
        English: Write page to cache; mark dirty for later flush.
        Chinese: 将页写入缓存，标记脏以便后续刷回。
        Japanese: ページをキャッシュに書き込み、後でフラッシュするためダーティにマーク。
        """
        if node.is_leaf and isinstance(node, LeafNode):
            prev_id = node.prev_id if node.prev_id >= 0 else INVALID_PAGE_ID
            next_id = node.next_id if node.next_id >= 0 else INVALID_PAGE_ID
            raw = serialize_leaf_page_slotted(
                [_ensure_int_key(k) for k in node.keys],
                [_ensure_str_value(v) for v in node.values],
                prev_id,
                next_id,
            )
        else:
            if not isinstance(node, InternalNode):
                raise TypeError("Expected InternalNode")
            raw = serialize_internal_page(
                [_ensure_int_key(k) for k in node.keys],
                [int(c) for c in node.children],
            )
        self._put_in_cache(page_id, raw, dirty=True)

    def flush_dirty_pages(self) -> int:
        """
        English: Flush only dirty pages to disk; return count flushed.
        Chinese: 仅刷写脏页到磁盘；返回刷写数量。
        Japanese: ダーティページのみをディスクにフラッシュ；フラッシュ数を返す。
        """
        if not self._filepath:
            return 0
        count = 0
        for page_id, (raw, dirty) in list(self._cache.items()):
            if dirty:
                self._write_page_to_disk(page_id, raw)
                self._cache[page_id] = (raw, False)
                count += 1
        if count > 0:
            self._save_header()
        return count

    def flush(self) -> None:
        """
        English: Atomically flush all dirty pages to disk; write to temp then rename.
        Chinese: 原子刷写：先写临时文件再重命名，确保崩溃安全。
        Japanese: アトミックフラッシュ：一時ファイルに書き込み後に rename、クラッシュ安全。
        """
        if not self._filepath:
            return
        self._filepath.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._filepath.with_suffix(self._filepath.suffix + ".tmp")
        try:
            with open(tmp_path, "wb") as f:
                f.write(b"B+DB")
                f.write(struct.pack("<i", self._root_page_id))
                f.write(struct.pack("<i", self._next_page_id))
                for page_id, (raw, dirty) in sorted(self._cache.items()):
                    f.seek(16 + page_id * PAGE_SIZE)
                    f.write(raw)
                    if dirty:
                        self._cache[page_id] = (raw, False)
            os.replace(tmp_path, self._filepath)
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
            raise
