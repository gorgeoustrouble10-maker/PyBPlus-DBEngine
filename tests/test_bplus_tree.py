"""
B+ 树结构测试：验证节点类实例化及基础 search/insert 行为。
"""

import tempfile
from pathlib import Path

import pytest

from bplus_tree.node import BPlusTreeNode, InternalNode, LeafNode
from bplus_tree.tree import BPlusTree


class TestNodeInstantiation:
    """节点类实例化测试。"""

    def test_internal_node_creation(self) -> None:
        """InternalNode 可正确实例化。"""
        node = InternalNode()
        assert node.is_leaf is False
        assert node.keys == []
        assert node.children == []
        assert node.is_full is False

    def test_leaf_node_creation(self) -> None:
        """LeafNode 可正确实例化。"""
        node = LeafNode()
        assert node.is_leaf is True
        assert node.keys == []
        assert node.values == []
        assert node.prev is None
        assert node.next is None
        assert node.is_full is False

    def test_internal_node_with_keys(self) -> None:
        """InternalNode 可持有 keys 和 children。"""
        node = InternalNode()
        node.keys = [10, 20, 30]
        node.children = [LeafNode(), LeafNode(), LeafNode(), LeafNode()]
        assert len(node.keys) == 3
        assert len(node.children) == 4

    def test_leaf_node_with_key_values(self) -> None:
        """LeafNode 可持有 key-value 对。"""
        node = LeafNode()
        node.keys = [1, 2, 3]
        node.values = ["a", "b", "c"]
        assert node.keys == [1, 2, 3]
        assert node.values == ["a", "b", "c"]


class TestBPlusTree:
    """BPlusTree 基础行为测试。"""

    def test_empty_tree_search_returns_none(self) -> None:
        """空树 search 返回 None。"""
        tree = BPlusTree()
        assert tree.search(42) is None

    def test_insert_and_search_single(self) -> None:
        """插入单个 key-value 后可正确 search。"""
        tree = BPlusTree()
        tree.insert(10, "v10")
        assert tree.search(10) == "v10"

    def test_insert_and_search_multiple(self) -> None:
        """插入多个 key-value 后可正确 search。"""
        tree = BPlusTree()
        for i in [3, 1, 4, 1, 5]:
            tree.insert(i, f"v{i}")
        assert tree.search(1) == "v1"
        assert tree.search(3) == "v3"
        assert tree.search(4) == "v4"
        assert tree.search(5) == "v5"

    def test_search_nonexistent_returns_none(self) -> None:
        """search 不存在的 key 返回 None。"""
        tree = BPlusTree()
        tree.insert(10, "v10")
        assert tree.search(9) is None
        assert tree.search(11) is None

    def test_insert_full_leaf_triggers_split(self) -> None:
        """插入导致叶子满时触发分裂，而非抛出异常。"""
        tree = BPlusTree(order=4)
        for i in range(5):
            tree.insert(i, f"v{i}")
        for i in range(5):
            assert tree.search(i) == f"v{i}"

    def test_insert_and_split(self) -> None:
        """连续插入 1 到 15，验证树高、根节点与叶子链完整性。"""
        tree = BPlusTree(order=4)
        for i in range(1, 16):
            tree.insert(i, f"v{i}")

        # 树高度应增加（至少 2 层）
        assert tree.height() >= 2

        # 叶子 next 指针链能按序遍历出 1 到 15
        keys_in_order = list(tree.iterate_leaf_keys())
        assert keys_in_order == list(range(1, 16))

        # 所有 key 均可正确 search
        for i in range(1, 16):
            assert tree.search(i) == f"v{i}"

    def test_delete_basic(self) -> None:
        """删除后 search 不到。"""
        tree = BPlusTree()
        tree.insert(10, "v10")
        tree.insert(20, "v20")
        tree.delete(10)
        assert tree.search(10) is None
        assert tree.search(20) == "v20"

        tree.delete(20)
        assert tree.search(20) is None
        assert tree.height() == 0

    def test_delete_nonexistent_raises(self) -> None:
        """删除不存在的 key 抛出 KeyError。"""
        tree = BPlusTree()
        tree.insert(10, "v10")
        with pytest.raises(KeyError, match="42"):
            tree.delete(42)

    def test_delete_and_rebalance(self) -> None:
        """插入 1-10，依次删除，验证树依然平衡且高度正确收缩。"""
        tree = BPlusTree(order=4)
        for i in range(1, 11):
            tree.insert(i, f"v{i}")

        initial_height = tree.height()
        assert initial_height >= 2

        # 依次删除 1 到 10
        for i in range(1, 11):
            tree.delete(i)
            assert tree.search(i) is None
            # 删除后其余 key 仍可查找
            for j in range(i + 1, 11):
                assert tree.search(j) == f"v{j}"

        # 全删后树为空
        assert tree.height() == 0
        assert list(tree.iterate_leaf_keys()) == []

    def test_range_scan(self) -> None:
        """范围查询能按序返回区间内的 key-value。"""
        tree = BPlusTree(order=4)
        for i in range(1, 21):
            tree.insert(i, f"v{i}")
        result = list(tree.range_scan(5, 15))
        assert result == [(i, f"v{i}") for i in range(5, 16)]

    def test_save_and_load(self) -> None:
        """持久化保存与加载后结构完整、prev/next 正确。"""
        tree = BPlusTree(order=4)
        for i in range(1, 16):
            tree.insert(i, f"v{i}")
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = Path(f.name)
        try:
            tree.save_to_file(path)
            loaded = BPlusTree.load_from_file(path)
            # 所有 key 可查
            for i in range(1, 16):
                assert loaded.search(i) == f"v{i}"
            # 叶子链按序
            keys = list(loaded.iterate_leaf_keys())
            assert keys == list(range(1, 16))
        finally:
            path.unlink(missing_ok=True)

    def test_to_mermaid(self) -> None:
        """to_mermaid 输出包含 flowchart 与节点信息。"""
        tree = BPlusTree(order=4)
        tree.insert(1, "a")
        tree.insert(2, "b")
        m = tree.to_mermaid()
        assert "flowchart" in m
        assert "1" in m or "2" in m

    def test_concurrent_inserts(self) -> None:
        """多线程并发插入，树结构正确无丢失。"""
        import concurrent.futures

        tree = BPlusTree(order=16, concurrent=True)
        n_per_thread = 100
        n_threads = 4

        def insert_range(start: int) -> None:
            for i in range(start, start + n_per_thread):
                tree.insert(i, f"v{i}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as ex:
            futures = [
                ex.submit(insert_range, t * n_per_thread) for t in range(n_threads)
            ]
            concurrent.futures.wait(futures)

        total = n_per_thread * n_threads
        for i in range(total):
            assert tree.search(i) == f"v{i}"
        keys = list(tree.iterate_leaf_keys())
        assert len(keys) == total
        assert sorted(keys) == list(range(total))

    def test_wal_recovery(self) -> None:
        """WAL 重放可恢复崩溃前状态。"""
        with tempfile.NamedTemporaryFile(suffix=".log", delete=False) as f:
            wal_path = Path(f.name)
        try:
            tree = BPlusTree(order=4, wal_path=wal_path)
            for i in range(1, 11):
                tree.insert(i, f"v{i}")
            recovered = BPlusTree.recover_from_wal(wal_path, order=4)
            for i in range(1, 11):
                assert recovered.search(i) == f"v{i}"
        finally:
            wal_path.unlink(missing_ok=True)

    def test_save_and_load_db(self) -> None:
        """二进制 .db 持久化与加载后结构完整。"""
        tree = BPlusTree(order=4)
        for i in range(1, 11):
            tree.insert(i, f"v{i}")
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            path = Path(f.name)
        try:
            tree.save_to_db(path)
            loaded = BPlusTree.load_from_db(path)
            for i in range(1, 11):
                assert loaded.search(i) == f"v{i}"
            keys = list(loaded.iterate_leaf_keys())
            assert keys == list(range(1, 11))
        finally:
            path.unlink(missing_ok=True)
