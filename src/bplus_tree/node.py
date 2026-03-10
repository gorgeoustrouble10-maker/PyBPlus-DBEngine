"""
B+ 树节点抽象：模拟磁盘页（Page）的结构。
支持 B-Link Tree 雏形：high_key、right_sibling。
"""

from abc import ABC, abstractmethod
from typing import Any, Optional

# 阶数 Order = 4：每个节点最多容纳 4 个 Key，便于测试分裂逻辑
ORDER: int = 4
MAX_KEYS: int = ORDER
MIN_KEYS: int = (ORDER + 1) // 2


class BPlusTreeNode(ABC):
    """
    English: B+ tree node base; supports B-Link high_key and right_sibling.
    Chinese: B+ 树节点基类；支持 B-Link 的 high_key 与 right_sibling。
    Japanese: B+ 木ノード基底；B-Link の high_key と right_sibling をサポート。
    """

    def __init__(self) -> None:
        self.keys: list[Any] = []
        # B-Link: 该节点内键的上界；分裂后用于路由决策
        self.high_key: Optional[Any] = None
        # B-Link: 分裂产生的右兄弟指针，父节点更新前可通过此链找到新区
        self.right_sibling: Optional[Any] = None

    @property
    @abstractmethod
    def is_leaf(self) -> bool:
        """Whether this node is a leaf / 是否为叶子节点。"""
        ...

    @property
    def is_full(self) -> bool:
        """Current page full (>= MAX_KEYS) / 当前页是否已满。"""
        return len(self.keys) >= MAX_KEYS

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(keys={self.keys}, high_key={self.high_key})"


class InternalNode(BPlusTreeNode):
    """
    内部节点：仅存索引 Key 和子节点指针。
    子节点数量 = keys 数量 + 1。
    Binary 模式：children 存 page_id (int)；In-memory 模式：存 BPlusTreeNode。
    """

    def __init__(self) -> None:
        super().__init__()
        self.children: list[Any] = []  # BPlusTreeNode 或 int (page_id)

    @property
    def is_leaf(self) -> bool:
        return False

    def __repr__(self) -> str:
        return f"InternalNode(keys={self.keys}, children_count={len(self.children)})"


class LeafNode(BPlusTreeNode):
    """
    叶子节点：存 Key-Value 对，并在同一层级形成双向链表，
    以支持高效的范围查询（Range Query）。
    In-memory: prev/next 为 LeafNode；Binary: prev_id/next_id 为 page_id。
    """

    def __init__(self) -> None:
        super().__init__()
        self.values: list[Any] = []
        self.prev: Optional[Any] = None  # LeafNode (in-memory)
        self.next: Optional[Any] = None
        # Binary 模式下使用，-1 表示无
        self.prev_id: int = -1
        self.next_id: int = -1

    @property
    def is_leaf(self) -> bool:
        return True

    def __repr__(self) -> str:
        return f"LeafNode(keys={self.keys}, values={self.values})"
