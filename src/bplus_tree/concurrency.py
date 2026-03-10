"""
并发控制：Latch Crabbing（锁螃蟹算法）。

English: Latch crabbing for safe B+ tree concurrency; release parent only when child is safe.
Chinese: 锁螃蟹：子节点被锁且确认安全后才释放父节点锁，防止死锁。
Japanese: ラッチクラッピング：子ノードがロックされ安全と確認してから親を解放、デッドロック防止。
"""

import threading
from contextlib import contextmanager
from typing import Any, Iterator, Optional

from bplus_tree.node import BPlusTreeNode, InternalNode, LeafNode
from bplus_tree.node import MAX_KEYS, MIN_KEYS


class LatchManager:
    """
    English: Per-node RLock manager for latch crabbing.
    Chinese: 每节点 RLock 管理，支持锁螃蟹遍历。
    Japanese: ノード単位 RLock 管理、ラッチクラッピング走査をサポート。
    """

    def __init__(self) -> None:
        self._latches: dict[int, threading.RLock] = {}
        self._lock = threading.Lock()

    def _get_latch(self, node: BPlusTreeNode) -> threading.RLock:
        """获取节点的 RLock，不存在则创建。"""
        nid = id(node)
        with self._lock:
            if nid not in self._latches:
                self._latches[nid] = threading.RLock()
            return self._latches[nid]

    def read_lock(self, node: BPlusTreeNode) -> None:
        """
        English: Acquire read lock (shared) on node.
        Chinese: 获取节点的读锁（共享）。
        Japanese: ノードの読取ロック（共有）を取得します。
        """
        self._get_latch(node).acquire()

    def read_unlock(self, node: BPlusTreeNode) -> None:
        """释放节点的读锁。"""
        self._get_latch(node).release()

    def write_lock(self, node: BPlusTreeNode) -> None:
        """
        English: Acquire write lock (exclusive) on node.
        Chinese: 获取节点的写锁（排他）。
        Japanese: ノードの書込ロック（排他）を取得します。
        """
        self._get_latch(node).acquire()

    def write_unlock(self, node: BPlusTreeNode) -> None:
        """释放节点的写锁。"""
        self._get_latch(node).release()

    @staticmethod
    def is_safe_for_insert(node: BPlusTreeNode) -> bool:
        """
        English: True if node won't split on one more insert.
        Chinese: 再插入一个 key 也不会分裂则为安全。
        Japanese: あと 1 キー挿入しても分割しないなら安全。
        """
        return bool(len(node.keys) < MAX_KEYS)

    @staticmethod
    def is_safe_for_delete(node: BPlusTreeNode) -> bool:
        """
        English: True if node won't merge on one more delete.
        Chinese: 再删除一个 key 也不会合并则为安全。
        Japanese: あと 1 キー削除してもマージしないなら安全。
        """
        return bool(len(node.keys) > MIN_KEYS)


class CrabbingGuard:
    """
    English: RAII guard for latch crabbing; releases ancestors when child is safe.
    Chinese: 锁螃蟹 RAII 护卫；子节点安全时释放祖先锁。
    Japanese: ラッチクラッピング RAII ガード；子が安全な時祖先ロックを解放。
    """

    def __init__(
        self,
        latch_mgr: LatchManager,
        mode: str,
        ancestors: list[tuple[BPlusTreeNode, bool]],
        current: BPlusTreeNode,
        is_safe: bool,
    ) -> None:
        self._mgr = latch_mgr
        self._mode = mode
        self._ancestors = ancestors
        self._current = current
        self._is_safe = is_safe
        self._released: list[BPlusTreeNode] = []

    def __enter__(self) -> "CrabbingGuard":
        if self._is_safe:
            for node, _ in self._ancestors:
                if self._mode == "read":
                    self._mgr.read_unlock(node)
                else:
                    self._mgr.write_unlock(node)
                self._released.append(node)
            self._ancestors.clear()
        return self

    def __exit__(self, *args: Any) -> None:
        if self._mode == "read":
            self._mgr.read_unlock(self._current)
        else:
            self._mgr.write_unlock(self._current)
        for node, _ in list(self._ancestors):
            if self._mode == "read":
                self._mgr.read_unlock(node)
            else:
                self._mgr.write_unlock(node)


class TreeLatch:
    """
    English: Tree-level RLock for insert/search/delete; crabbing uses per-node latches when enabled.
    Chinese: 树级 RLock，用于 insert/search/delete；启用时可配合每节点锁做锁螃蟹。
    Japanese: 木全体の RLock、insert/search/delete 用；有効時はノード単位鎖でクラッピング。
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._latch_mgr = LatchManager()

    @contextmanager
    def read_guard(self) -> Iterator[None]:
        """
        English: Acquire read lock for search.
        Chinese: 为 search 获取读锁。
        Japanese: search 用に読取ロックを取得します。
        """
        self._lock.acquire()
        try:
            yield
        finally:
            self._lock.release()

    @contextmanager
    def write_guard(self) -> Iterator[None]:
        """
        English: Acquire write lock for insert/delete.
        Chinese: 为 insert/delete 获取写锁。
        Japanese: insert/delete 用に書込ロックを取得します。
        """
        self._lock.acquire()
        try:
            yield
        finally:
            self._lock.release()

    @property
    def latch_mgr(self) -> LatchManager:
        """Per-node latch manager for fine-grained crabbing."""
        return self._latch_mgr
