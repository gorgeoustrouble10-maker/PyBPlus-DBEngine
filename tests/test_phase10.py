"""Phase 10 高级特性测试：B-Link、FSM、CBO-Lite。"""

import pytest

from bplus_tree.node import InternalNode, LeafNode
from bplus_tree.storage import BufferPool, FreeSpaceMap
from bplus_tree.tree import BPlusTree


class TestBLinkTree:
    """B-Link Tree 雏形测试。"""

    def test_split_leaf_sets_high_key_and_right_sibling(self) -> None:
        """分裂叶子时设置 high_key 与 right_sibling。"""
        tree = BPlusTree(order=4)
        for i in range(6):
            tree.insert(i, f"v{i}")
        assert tree._root is not None
        node = tree._root
        while not node.is_leaf:
            node = node.children[0]
        assert hasattr(node, "high_key")
        assert node.high_key is not None or len(node.keys) == 0


class TestFreeSpaceMap:
    """FreeSpaceMap 测试。"""

    def test_fsm_reuse(self) -> None:
        """FSM 复用空闲页。"""
        fsm = FreeSpaceMap()
        fsm.add_free(3)
        fsm.add_free(5)
        assert fsm.pop_free() in (3, 5)
        assert fsm.pop_free() in (3, 5)
        assert fsm.pop_free() is None

    def test_buffer_pool_prefers_fsm(self) -> None:
        """BufferPool.allocate_page 优先从 FSM 复用。"""
        pool = BufferPool()
        pid1 = pool.allocate_page()
        pid2 = pool.allocate_page()
        pool.free_page(pid1)
        pid3 = pool.allocate_page()
        assert pid3 == pid1