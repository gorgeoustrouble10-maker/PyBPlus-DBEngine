"""
B+ 树核心实现：基于模拟磁盘页的索引结构。

English: Core B+ tree implementation with page-based disk simulation.
Chinese: B+ 树核心实现，基于模拟磁盘页的索引结构。
Japanese: ページベースのディスクシミュレーションに基づく B+ 木コア実装。
"""

import bisect
import json
from pathlib import Path
from typing import Any, Iterator, Optional

from bplus_tree.concurrency import TreeLatch
from bplus_tree.logging import WriteAheadLog
from bplus_tree.node import (
    BPlusTreeNode,
    InternalNode,
    LeafNode,
    MAX_KEYS,
    MIN_KEYS,
)
from bplus_tree.storage import BufferPool, INVALID_PAGE_ID


class BPlusTree:
    """
    English: B+ tree simulating disk pages for database indexing.
    Chinese: B+ 树：模拟磁盘页的数据库索引结构，每个节点对应一个 Page。
    Japanese: B+ 木：ディスクページをシミュレートするデータベースインデックス構造。
    """

    def __init__(
        self,
        order: int = 4,
        concurrent: bool = False,
        wal_path: Optional[Path] = None,
    ) -> None:
        """
        English: Initialize the B+ tree; optionally enable concurrency (RLock) and WAL.
        Chinese: 初始化 B+ 树；可选启用并发（RLock）与预写日志。
        Japanese: B+ 木を初期化；オプションで並行制御（RLock）と WAL を有効化。

        Args:
            order: Maximum keys per node (default 4).
            concurrent: If True, use TreeLatch for thread-safe insert/search/delete.
            wal_path: If set, log INSERT/DELETE before modifications for crash recovery.
        """
        self._order: int = order
        self._root: Optional[BPlusTreeNode] = None
        self._concurrent: bool = concurrent
        self._latch: Optional[TreeLatch] = TreeLatch() if concurrent else None
        self._wal: Optional[WriteAheadLog] = (
            WriteAheadLog(wal_path) if wal_path else None
        )
        self._pool: Optional[BufferPool] = None
        self._node_to_pid: dict[int, int] = {}

    def search(self, key: Any) -> Optional[Any]:
        """
        English: Point search: find value by key.
        Chinese: 点查：根据 key 查找对应的 value。
        Japanese: ポイント検索：キーにより値を検索します。

        Args:
            key: Key to search for.

        Returns:
            Value if found, else None.
        """
        if self._latch is not None:
            with self._latch.read_guard():
                return self._search_impl(key)
        return self._search_impl(key)

    def _search_impl(self, key: Any) -> Optional[Any]:
        """search 的实现，无锁包装。"""
        if self._root is None:
            return None

        node: BPlusTreeNode = self._root

        # 1. 自根向下遍历至叶子节点
        while not node.is_leaf:
            internal = node
            if not isinstance(internal, InternalNode):
                raise TypeError("Expected InternalNode")
            # 2. 找到第一个 >= key 的位置，走对应子节点
            i = 0
            while i < len(internal.keys) and key >= internal.keys[i]:
                i += 1
            node = internal.children[i]

        # 3. 到达叶子，二分查找
        leaf = node
        if not isinstance(leaf, LeafNode):
            raise TypeError("Expected LeafNode")
        idx = bisect.bisect_left(leaf.keys, key)
        if idx < len(leaf.keys) and leaf.keys[idx] == key:
            return leaf.values[idx]
        return None

    def insert(self, key: Any, value: Any) -> None:
        """
        English: Insert a key-value pair; trigger split when node overflows.
        Chinese: 插入键值对，并在节点溢出时触发分裂。
        Japanese: キーと値のペアを挿入し、ノード溢出時に分割をトリガーします。

        Args:
            key: Key to insert.
            value: Value to insert.
        """
        if self._wal is not None:
            self._wal.log_insert(key, value)
        if self._latch is not None:
            with self._latch.write_guard():
                self._insert_impl(key, value)
            return
        self._insert_impl(key, value)

    def _insert_impl(self, key: Any, value: Any) -> None:
        """insert 的实现，无锁包装。"""
        if self._root is None:
            leaf = LeafNode()
            _insert_into_leaf(leaf, key, value)
            self._root = leaf
            return

        # 1. 从根向下查找目标叶子，并记录路径（用于分裂时回溯父节点）
        path: list[tuple[InternalNode, int]] = []
        node: BPlusTreeNode = self._root

        while not node.is_leaf:
            internal = node
            if not isinstance(internal, InternalNode):
                raise TypeError("Expected InternalNode")
            # 2. 确定下行子节点索引
            i = 0
            while i < len(internal.keys) and key >= internal.keys[i]:
                i += 1
            path.append((internal, i))
            node = internal.children[i]

        leaf = node
        if not isinstance(leaf, LeafNode):
            raise TypeError("Expected LeafNode")

        _insert_into_leaf(leaf, key, value)
        if leaf.keys:
            leaf.high_key = max(leaf.keys)

        # 4. 若溢出则递归分裂
        current: BPlusTreeNode = leaf
        while len(current.keys) > MAX_KEYS:
            if current.is_leaf:
                if not isinstance(current, LeafNode):
                    raise TypeError("Expected LeafNode")
                push_key, right_sibling = _split_leaf(current)
            else:
                if not isinstance(current, InternalNode):
                    raise TypeError("Expected InternalNode")
                push_key, right_sibling = _split_internal(current)

            # 5. 处理根分裂或向父节点上提
            if not path:
                new_root = InternalNode()
                new_root.keys = [push_key]
                new_root.children = [current, right_sibling]
                new_root.high_key = push_key
                self._root = new_root
                return

            parent, child_idx = path.pop()
            parent.keys.insert(child_idx, push_key)
            parent.children.insert(child_idx + 1, right_sibling)
            parent.high_key = max(parent.keys) if parent.keys else push_key
            current = parent

    def height(self) -> int:
        """
        English: Return the height of the tree (1 for single leaf).
        Chinese: 返回树高，单叶子时为 1。
        Japanese: 木の高さを返す（単一葉の場合は 1）。

        Returns:
            Tree height.
        """
        if self._root is None:
            return 0
        h = 1
        node: BPlusTreeNode = self._root
        while not node.is_leaf:
            if not isinstance(node, InternalNode):
                raise TypeError("Expected InternalNode")
            node = node.children[0]
            h += 1
        return h

    def iterate_leaf_keys(self) -> Iterator[Any]:
        """
        English: Iterate all keys in order via leaf next-pointer chain.
        Chinese: 沿叶子节点 next 指针链按序迭代所有 key。
        Japanese: 葉の next ポインタチェーンで全キーを順序通りにイテレートします。

        Yields:
            Keys in ascending order.
        """
        if self._root is None:
            return

        # 1. 找到最左叶子
        node: BPlusTreeNode = self._root
        while not node.is_leaf:
            if not isinstance(node, InternalNode):
                raise TypeError("Expected InternalNode")
            node = node.children[0]

        leaf = node
        if not isinstance(leaf, LeafNode):
            raise TypeError("Expected LeafNode")

        # 2. 沿 next 链遍历
        while leaf is not None:
            for k in leaf.keys:
                yield k
            leaf = leaf.next

    def delete(self, key: Any) -> None:
        """
        English: Delete key and rebalance if node underflows.
        Chinese: 删除键并在节点欠键时再平衡。
        Japanese: キーを削除し、ノードがキー不足になった場合は再平衡化します。

        Args:
            key: Key to delete.

        Raises:
            KeyError: If key does not exist.
        """
        if self._root is None:
            raise KeyError(key)
        if self._latch is not None:
            with self._latch.write_guard():
                self._delete_impl(key)
            return
        self._delete_impl(key)

    def _delete_impl(self, key: Any) -> None:
        """delete 的实现，无锁包装。"""
        if self._root is None:
            raise KeyError(key)

        # 1. 点查找到目标叶子并记录路径
        leaf, path = _find_leaf_with_path(self._root, key)
        idx = bisect.bisect_left(leaf.keys, key)
        if idx >= len(leaf.keys) or leaf.keys[idx] != key:
            raise KeyError(key)
        if self._wal is not None:
            self._wal.log_delete(key)

        # 2. 删除该 key-value 对
        leaf.keys.pop(idx)
        leaf.values.pop(idx)

        # 3. 若删除后叶子为空且为根，置空树；该叶页归还 FSM
        if leaf is self._root and len(leaf.keys) == 0:
            _try_free_discarded_node(self, leaf)
            self._root = None
            return

        # 4. 若叶子欠键且非根，触发再平衡
        if len(leaf.keys) < MIN_KEYS and leaf is not self._root and path:
            _rebalance(self, leaf, path)

    def range_scan(self, start_key: Any, end_key: Any) -> Iterator[tuple[Any, Any]]:
        """
        English: Range scan: yield (key, value) for keys in [start_key, end_key].
        Chinese: 范围查询：迭代 [start_key, end_key] 内的 (key, value)。
        Japanese: 範囲スキャン：[start_key, end_key] 内の (key, value) をイテレートします。

        Yields:
            (key, value) tuples in ascending key order.
        """
        if self._root is None:
            return
        # 1. 找到包含 start_key 的叶子
        leaf, _ = _find_leaf_with_path(self._root, start_key)
        if not isinstance(leaf, LeafNode):
            raise TypeError("Expected LeafNode")
        # 2. 从 start_key 所在位置开始，沿 next 链扫描至 end_key
        while leaf is not None:
            for i, k in enumerate(leaf.keys):
                if k > end_key:
                    return
                if k >= start_key:
                    yield k, leaf.values[i]
            leaf = leaf.next

    def save_to_file(self, filepath: str | Path) -> None:
        """
        English: Persist tree to JSON file; keys/values must be JSON-serializable.
        Chinese: 将树持久化到 JSON 文件；键值须可 JSON 序列化。
        Japanese: 木を JSON ファイルに永続化；キー・値は JSON シリアライズ可能であること。

        Args:
            filepath: Output file path.
        """
        path = Path(filepath)
        # 1. 遍历树，为节点分配 id 并序列化
        nodes_data, root_id = _serialize_tree(self._root, self._order)
        payload = {"order": self._order, "root_id": root_id, "nodes": nodes_data}
        with path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    @classmethod
    def load_from_file(cls, filepath: str | Path) -> "BPlusTree":
        """
        English: Load tree from JSON file; restores structure and prev/next pointers.
        Chinese: 从 JSON 文件加载树；恢复结构及 prev/next 指针。
        Japanese: JSON ファイルから木をロード；構造と prev/next ポインタを復元します。

        Args:
            filepath: Input file path.

        Returns:
            Restored BPlusTree instance.
        """
        path = Path(filepath)
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        # 1. 重建节点列表
        order = int(payload["order"])
        root_id = payload["root_id"]
        nodes = _deserialize_nodes(payload["nodes"])
        # 2. 连接 prev/next 并设置根
        tree = cls(order=order)
        tree._root = nodes[root_id] if root_id >= 0 else None
        return tree

    def save_to_db(self, filepath: str | Path) -> None:
        """
        English: Persist tree to binary .db file using Buffer Pool.
        Chinese: 将树持久化到二进制 .db 文件，采用 Buffer Pool。
        Japanese: 木を Buffer Pool によりバイナリ .db ファイルに永続化します。

        Args:
            filepath: Output .db file path.
        """
        path = Path(filepath)
        pool = getattr(self, "_pool", None)
        if pool is None or (pool._filepath and str(pool._filepath) != str(path)):
            pool = BufferPool(path)
        _persist_tree_to_pool(self._root, pool, self._order)
        pool.flush()

    @classmethod
    def recover_from_wal(cls, wal_path: str | Path, order: int = 4) -> "BPlusTree":
        """
        English: Recover tree by replaying WAL after crash.
        Chinese: 崩溃后通过重放 WAL 恢复树。
        Japanese: クラッシュ後、WAL をリプレイして木を復旧します。

        Returns:
            Recovered BPlusTree.
        """
        tree = cls(order=order)
        for op, key, value in WriteAheadLog.replay(wal_path):
            if op == "INSERT" and value is not None:
                tree.insert(key, value)
            elif op == "DELETE":
                try:
                    tree.delete(key)
                except KeyError:
                    pass
        return tree

    @classmethod
    def load_from_db(
        cls,
        filepath: str | Path,
        keep_pool: bool = True,
    ) -> "BPlusTree":
        """
        English: Load tree from binary .db file; optionally keep pool for FSM closure.
        Chinese: 从二进制 .db 文件加载树；可选保持 pool 以实现 FSM 闭环。
        Japanese: バイナリ .db ファイルから木をロード；オプションで pool を保持し FSM を閉じる。

        Args:
            keep_pool: If True, tree keeps pool ref and node_to_pid; merge will free pages to FSM.
        """
        path = Path(filepath)
        pool = BufferPool(path)
        rid = pool.get_root_page_id()
        tree = cls()
        if rid < 0:
            return tree
        root, pid_to_node = _reconstruct_tree_from_pool(rid, pool)
        tree._root = root
        if keep_pool:
            tree._pool = pool
            tree._node_to_pid = {id(n): p for p, n in pid_to_node.items()}
        return tree

    def to_mermaid(self) -> str:
        """
        English: Export tree structure as Mermaid flowchart text for visualization.
        Chinese: 将树结构导出为 Mermaid 流程图文本，便于可视化。
        Japanese: 木構造を Mermaid フローチャートテキストとしてエクスポートします。

        Returns:
            Mermaid diagram string.
        """
        if self._root is None:
            return "flowchart TD\n    empty[空树]\n"
        lines = ["flowchart TD"]
        # 1. 根节点单独输出，再递归子节点
        _mermaid_visit(self._root, lines, "ROOT", is_root=True)
        return "\n".join(lines)


def _serialize_tree(
    root: Optional[BPlusTreeNode], order: int
) -> tuple[list[dict[str, Any]], int]:
    """序列化树（简化实现）：BFS 收集节点，填好 children_ids 与 prev/next_id。"""
    if root is None:
        return [], -1
    # 1. BFS 收集所有节点并分配 id
    all_nodes: list[BPlusTreeNode] = []
    seen: set[int] = set()
    q: list[BPlusTreeNode] = [root]
    while q:
        n = q.pop(0)
        if id(n) in seen:
            continue
        seen.add(id(n))
        all_nodes.append(n)
        if n.is_leaf and isinstance(n, LeafNode):
            if n.next and id(n.next) not in seen:
                q.append(n.next)
        elif not n.is_leaf and isinstance(n, InternalNode):
            for c in n.children:
                if id(c) not in seen:
                    q.append(c)
    node_to_id = {id(n): i for i, n in enumerate(all_nodes)}
    # 2. 叶子按 next 链排序
    leaves: list[LeafNode] = []
    node = root
    while not node.is_leaf:
        if not isinstance(node, InternalNode):
            raise TypeError("Expected InternalNode")
        node = node.children[0]
    cur: Optional[LeafNode] = node if isinstance(node, LeafNode) else None
    while cur is not None:
        leaves.append(cur)
        cur = cur.next
    leaf_ids = [node_to_id[id(l)] for l in leaves]
    # 3. 生成 nodes_data
    nodes_data: list[dict[str, Any]] = []
    leaf_pos = {lid: i for i, lid in enumerate(leaf_ids)}
    for i, n in enumerate(all_nodes):
        if n.is_leaf and isinstance(n, LeafNode):
            idx = leaf_pos.get(i, 0)
            prev_id = leaf_ids[idx - 1] if idx > 0 else -1
            next_id = leaf_ids[idx + 1] if idx + 1 < len(leaf_ids) else -1
            nodes_data.append(
                {
                    "id": i,
                    "is_leaf": True,
                    "keys": n.keys,
                    "values": n.values,
                    "prev_id": prev_id,
                    "next_id": next_id,
                }
            )
        else:
            internal = n
            if not isinstance(internal, InternalNode):
                raise TypeError("Expected InternalNode")
            children_ids = [node_to_id[id(c)] for c in internal.children]
            nodes_data.append(
                {
                    "id": i,
                    "is_leaf": False,
                    "keys": internal.keys,
                    "children_ids": children_ids,
                }
            )
    return nodes_data, 0


def _deserialize_nodes(nodes_data: list[dict[str, Any]]) -> list[BPlusTreeNode]:
    """从 nodes_data 反序列化出节点列表，并恢复 prev/next。"""
    nodes: list[BPlusTreeNode] = []
    # 1. 创建节点
    for nd in nodes_data:
        if nd.get("is_leaf"):
            ln = LeafNode()
            ln.keys = list(nd["keys"])
            ln.values = list(nd["values"])
            nodes.append(ln)
        else:
            internal = InternalNode()
            internal.keys = list(nd["keys"])
            nodes.append(internal)
    # 2. 为内部节点填充 children
    for nd, node in zip(nodes_data, nodes):
        if not nd.get("is_leaf") and isinstance(node, InternalNode):
            node.children = [nodes[cid] for cid in nd["children_ids"]]
    # 3. 为叶子填充 prev/next
    for nd, node in zip(nodes_data, nodes):
        if nd.get("is_leaf") and isinstance(node, LeafNode):
            prev_id = nd.get("prev_id", -1)
            next_id = nd.get("next_id", -1)
            node.prev = nodes[prev_id] if prev_id >= 0 else None
            node.next = nodes[next_id] if next_id >= 0 else None
    return nodes


def _mermaid_visit(
    node: BPlusTreeNode,
    lines: list[str],
    parent_name: str,
    is_root: bool = False,
) -> str:
    """
    English: Recursively generate Mermaid nodes and edges.
    Chinese: 递归生成 Mermaid 节点与边。
    Japanese: 再帰的に Mermaid ノードと辺を生成します。
    """
    nid = str(id(node))[-6:]
    label = ",".join(str(k) for k in node.keys[:5])
    if len(node.keys) > 5:
        label += "..."
    if node.is_leaf:
        name = f"N{nid}_L"
        lines.append(f"    {name}[{label}]")
        if not is_root:
            lines.append(f"    {parent_name} --> {name}")
        return name
    internal = node
    if not isinstance(internal, InternalNode):
        return ""
    name = f"N{nid}_I"
    lines.append(f"    {name}[{label}]")
    if not is_root:
        lines.append(f"    {parent_name} --> {name}")
    for c in internal.children:
        _mermaid_visit(c, lines, name)
    return name


def _persist_tree_to_pool(
    root: Optional[BPlusTreeNode],
    pool: BufferPool,
    order: int,
) -> None:
    """将 in-memory 树序列化到 BufferPool。"""
    if root is None:
        return
    # 1. BFS 收集所有节点并分配 page_id
    node_to_pid: dict[int, int] = {}
    q: list[BPlusTreeNode] = [root]
    while q:
        n = q.pop(0)
        if id(n) in node_to_pid:
            continue
        pid = pool.allocate_page()
        node_to_pid[id(n)] = pid
        if n.is_leaf and isinstance(n, LeafNode):
            nxt = n.next
            if isinstance(nxt, LeafNode) and id(nxt) not in node_to_pid:
                q.append(nxt)
        elif isinstance(n, InternalNode):
            for c in n.children:
                if isinstance(c, BPlusTreeNode) and id(c) not in node_to_pid:
                    q.append(c)
    # 2. 叶子 prev_id/next_id
    leaves: list[LeafNode] = []
    node = root
    while not node.is_leaf and isinstance(node, InternalNode):
        node = node.children[0]
    cur = node if isinstance(node, LeafNode) else None
    while cur is not None:
        leaves.append(cur)
        nxt = cur.next
        cur = nxt if isinstance(nxt, LeafNode) else None
    for i, ln in enumerate(leaves):
        ln.prev_id = node_to_pid[id(leaves[i - 1])] if i > 0 else -1
        ln.next_id = node_to_pid[id(leaves[i + 1])] if i + 1 < len(leaves) else -1
    # 3. 写入所有节点
    written: set[int] = set()

    def write_node(n: BPlusTreeNode) -> None:
        if id(n) in written:
            return
        if n.is_leaf and isinstance(n, LeafNode):
            pool.put_page(node_to_pid[id(n)], n)
            written.add(id(n))
        else:
            if isinstance(n, InternalNode):
                for c in n.children:
                    if isinstance(c, BPlusTreeNode):
                        write_node(c)
                orig = list(n.children)
                n.children = [
                    node_to_pid[id(c)] for c in orig if isinstance(c, BPlusTreeNode)
                ]
                pool.put_page(node_to_pid[id(n)], n)
                n.children = orig
                written.add(id(n))

    write_node(root)
    pool.set_root_page_id(node_to_pid[id(root)])


def _try_free_discarded_node(tree: "BPlusTree", node: BPlusTreeNode) -> None:
    """
    English: If tree has pool and node has page_id, free the page to FSM.
    Chinese: 若树持有 pool 且节点有 page_id，将页归还 FSM。
    Japanese: 木が pool を持ち、ノードに page_id があれば、ページを FSM に返却。
    """
    if tree._pool is not None:
        pid = tree._node_to_pid.get(id(node))
        if pid is not None:
            tree._pool.free_page(pid)
            del tree._node_to_pid[id(node)]


def _reconstruct_tree_from_pool(root_page_id: int, pool: BufferPool) -> tuple[BPlusTreeNode, dict[int, int]]:
    """从 pool 重建整棵树，避免 prev/next 循环引用。"""
    pid_to_node: dict[int, BPlusTreeNode] = {}
    stack = [root_page_id]
    # 1. 收集所有 page_id（BFS 内部节点 + 沿叶子链）
    all_pids: set[int] = {root_page_id}
    while stack:
        pid = stack.pop(0)
        page = pool.get_page(pid)
        if page.is_leaf and isinstance(page, LeafNode):
            if page.next_id >= 0 and page.next_id not in all_pids:
                all_pids.add(page.next_id)
                stack.append(page.next_id)
        elif isinstance(page, InternalNode):
            for cpid in page.children:
                if cpid not in all_pids:
                    all_pids.add(cpid)
                    stack.append(cpid)
    # 2. 创建所有节点
    for pid in all_pids:
        page = pool.get_page(pid)
        if page.is_leaf and isinstance(page, LeafNode):
            ln = LeafNode()
            ln.keys = list(page.keys)
            ln.values = list(page.values)
            pid_to_node[pid] = ln
        else:
            if isinstance(page, InternalNode):
                inn = InternalNode()
                inn.keys = list(page.keys)
                pid_to_node[pid] = inn
    # 3. 填充 children 和 prev/next
    stack = [root_page_id]
    seen: set[int] = set()
    while stack:
        pid = stack.pop(0)
        if pid in seen:
            continue
        seen.add(pid)
        page = pool.get_page(pid)
        node = pid_to_node[pid]
        if node.is_leaf and isinstance(node, LeafNode) and isinstance(page, LeafNode):
            node.prev = pid_to_node[page.prev_id] if page.prev_id >= 0 else None
            node.next = pid_to_node[page.next_id] if page.next_id >= 0 else None
        elif isinstance(node, InternalNode) and isinstance(page, InternalNode):
            node.children = [pid_to_node[cpid] for cpid in page.children]
            for cpid in page.children:
                if cpid not in seen:
                    stack.append(cpid)
    return pid_to_node[root_page_id], pid_to_node


def _find_leaf_with_path(
    root: BPlusTreeNode, key: Any
) -> tuple[LeafNode, list[tuple[InternalNode, int]]]:
    """
    English: Find leaf containing key and path from root.
    Chinese: 查找包含 key 的叶子节点及从根到其父的路径。
    Japanese: キーを含む葉ノードと根からその親までのパスを検索します。

    Returns:
        (leaf, path) where path[i] = (parent, child_index).

    Raises:
        TypeError: If structure is invalid.
    """
    path: list[tuple[InternalNode, int]] = []
    node: BPlusTreeNode = root

    while not node.is_leaf:
        internal = node
        if not isinstance(internal, InternalNode):
            raise TypeError("Expected InternalNode")
        i = 0
        while i < len(internal.keys) and key >= internal.keys[i]:
            i += 1
        path.append((internal, i))
        node = internal.children[i]

    leaf = node
    if not isinstance(leaf, LeafNode):
        raise TypeError("Expected LeafNode")
    return leaf, path


def _rebalance(
    tree: "BPlusTree",
    node: BPlusTreeNode,
    path: list[tuple[InternalNode, int]],
) -> None:
    """
    English: Rebalance underflowed node; borrow or merge with siblings.
    Chinese: 对欠键节点再平衡；优先借位，否则与兄弟合并。
    Japanese: キー不足ノードを再平衡化；借入を優先、否則は兄弟とマージします。
    """
    if not path:
        return
    parent, child_idx = path[-1]

    # 尝试向左兄弟借位
    if child_idx > 0:
        left_sibling = parent.children[child_idx - 1]
        if len(left_sibling.keys) > MIN_KEYS:
            if (
                node.is_leaf
                and isinstance(node, LeafNode)
                and isinstance(left_sibling, LeafNode)
            ):
                _borrow_from_left_leaf(parent, child_idx, left_sibling, node)
            elif (
                not node.is_leaf
                and isinstance(node, InternalNode)
                and isinstance(left_sibling, InternalNode)
            ):
                _borrow_from_left_internal(parent, child_idx, left_sibling, node)
            return

    # 尝试向右兄弟借位
    if child_idx + 1 < len(parent.children):
        right_sibling = parent.children[child_idx + 1]
        if len(right_sibling.keys) > MIN_KEYS:
            if (
                node.is_leaf
                and isinstance(node, LeafNode)
                and isinstance(right_sibling, LeafNode)
            ):
                _borrow_from_right_leaf(parent, child_idx, node, right_sibling)
            elif (
                not node.is_leaf
                and isinstance(node, InternalNode)
                and isinstance(right_sibling, InternalNode)
            ):
                _borrow_from_right_internal(parent, child_idx, node, right_sibling)
            return

    # 无法借位，执行合并（优先与左兄弟合并）
    if node.is_leaf:
        left_leaf = parent.children[child_idx - 1]
        right_leaf = parent.children[child_idx + 1]
        if (
            child_idx > 0
            and isinstance(left_leaf, LeafNode)
            and isinstance(node, LeafNode)
        ):
            _merge_leaf_left(tree, parent, child_idx, left_leaf, node, path)
        elif (
            child_idx + 1 < len(parent.children)
            and isinstance(right_leaf, LeafNode)
            and isinstance(node, LeafNode)
        ):
            _merge_leaf_right(tree, parent, child_idx, node, right_leaf, path)
    else:
        if child_idx > 0:
            _merge_internal_left(tree, parent, child_idx, path)
        else:
            _merge_internal_right(tree, parent, child_idx, path)


def _borrow_from_left_leaf(
    parent: InternalNode,
    child_idx: int,
    left: LeafNode,
    node: LeafNode,
) -> None:
    """
    English: Borrow rightmost key-value from left leaf into node's left.
    Chinese: 向左兄弟叶子借位：取左兄弟最右 key-value 插入当前节点最左。
    Japanese: 左兄弟の葉から最右キー値を借り、現ノードの最左に挿入します。
    """
    k, v = left.keys[-1], left.values[-1]
    left.keys.pop()
    left.values.pop()
    node.keys.insert(0, k)
    node.values.insert(0, v)
    parent.keys[child_idx - 1] = k


def _borrow_from_right_leaf(
    parent: InternalNode,
    child_idx: int,
    node: LeafNode,
    right: LeafNode,
) -> None:
    """
    English: Borrow leftmost key-value from right leaf into node's right.
    Chinese: 向右兄弟叶子借位：取右兄弟最左 key-value 插入当前节点最右。
    Japanese: 右兄弟の葉から最左キー値を借り、現ノードの最右に挿入します。
    """
    k, v = right.keys[0], right.values[0]
    right.keys.pop(0)
    right.values.pop(0)
    node.keys.append(k)
    node.values.append(v)
    parent.keys[child_idx] = right.keys[0] if right.keys else k


def _borrow_from_left_internal(
    parent: InternalNode,
    child_idx: int,
    left: InternalNode,
    node: InternalNode,
) -> None:
    """
    English: Borrow from left internal: pull down separator, take left's rightmost child.
    Chinese: 向左兄弟内部节点借位：拉下分隔键，取左兄弟最右子为当前最左子。
    Japanese: 左兄弟内部ノードから借入：区切りキーを下げ、左の最右子を取得。
    """
    sep = parent.keys[child_idx - 1]
    borrowed_child = left.children[-1]
    borrowed_key = left.keys[-1]
    left.keys.pop()
    left.children.pop()
    parent.keys[child_idx - 1] = borrowed_key
    node.keys.insert(0, sep)
    node.children.insert(0, borrowed_child)


def _borrow_from_right_internal(
    parent: InternalNode,
    child_idx: int,
    node: InternalNode,
    right: InternalNode,
) -> None:
    """
    English: Borrow from right internal: pull down separator, take right's leftmost child.
    Chinese: 向右兄弟内部节点借位：拉下分隔键，取右兄弟最左子为当前最右子。
    Japanese: 右兄弟内部ノードから借入：区切りキーを下げ、右の最左子を取得。
    """
    sep = parent.keys[child_idx]
    borrowed_child = right.children[0]
    borrowed_key = right.keys[0]
    right.keys.pop(0)
    right.children.pop(0)
    parent.keys[child_idx] = right.keys[0] if right.keys else sep
    node.keys.append(sep)
    node.children.append(borrowed_child)


def _merge_leaf_left(
    tree: "BPlusTree",
    parent: InternalNode,
    child_idx: int,
    left: LeafNode,
    node: LeafNode,
    path: list[tuple[InternalNode, int]],
) -> None:
    """
    English: Merge node into left leaf sibling.
    Chinese: 与左兄弟叶子合并：将当前节点内容并入左兄弟。
    Japanese: 左兄弟の葉とマージ：現ノードの内容を左に統合します。
    """
    left.keys.extend(node.keys)
    left.values.extend(node.values)
    left.next = node.next
    if node.next is not None:
        node.next.prev = left
    parent.keys.pop(child_idx - 1)
    parent.children.pop(child_idx)
    _try_free_discarded_node(tree, node)
    _rebalance_parent(tree, parent, path[:-1])


def _merge_leaf_right(
    tree: "BPlusTree",
    parent: InternalNode,
    child_idx: int,
    node: LeafNode,
    right: LeafNode,
    path: list[tuple[InternalNode, int]],
) -> None:
    """
    English: Merge node into right leaf sibling.
    Chinese: 与右兄弟叶子合并：将当前节点内容并入右兄弟。
    Japanese: 右兄弟の葉とマージ：現ノードの内容を右に統合します。
    """
    right.keys = node.keys + right.keys
    right.values = node.values + right.values
    right.prev = node.prev
    if node.prev is not None:
        node.prev.next = right
    parent.keys.pop(child_idx)
    parent.children.pop(child_idx)
    _try_free_discarded_node(tree, node)
    _rebalance_parent(tree, parent, path[:-1])


def _merge_internal_left(
    tree: "BPlusTree",
    parent: InternalNode,
    child_idx: int,
    path: list[tuple[InternalNode, int]],
) -> None:
    """
    English: Merge internal node into left internal sibling.
    Chinese: 与左兄弟内部节点合并。
    Japanese: 左兄弟内部ノードとマージします。
    """
    left = parent.children[child_idx - 1]
    node = parent.children[child_idx]
    if not isinstance(left, InternalNode) or not isinstance(node, InternalNode):
        raise TypeError("Expected InternalNode")
    sep = parent.keys[child_idx - 1]
    left.keys.append(sep)
    left.keys.extend(node.keys)
    left.children.extend(node.children)
    parent.keys.pop(child_idx - 1)
    parent.children.pop(child_idx)
    _try_free_discarded_node(tree, node)
    _rebalance_parent(tree, parent, path[:-1])


def _merge_internal_right(
    tree: "BPlusTree",
    parent: InternalNode,
    child_idx: int,
    path: list[tuple[InternalNode, int]],
) -> None:
    """
    English: Merge internal node into right internal sibling.
    Chinese: 与右兄弟内部节点合并。
    Japanese: 右兄弟内部ノードとマージします。
    """
    node = parent.children[child_idx]
    right = parent.children[child_idx + 1]
    if not isinstance(node, InternalNode) or not isinstance(right, InternalNode):
        raise TypeError("Expected InternalNode")
    sep = parent.keys[child_idx]
    node.keys.append(sep)
    node.keys.extend(right.keys)
    node.children.extend(right.children)
    parent.keys.pop(child_idx)
    parent.children.pop(child_idx)
    _try_free_discarded_node(tree, right)
    _rebalance_parent(tree, parent, path[:-1])


def _rebalance_parent(
    tree: "BPlusTree",
    parent: InternalNode,
    path: list[tuple[InternalNode, int]],
) -> None:
    """
    English: Rebalance parent after merge; collapse root if it has one child.
    Chinese: 合并后若父欠键则递归再平衡；根仅剩一子时收缩为子，树高 -1。
    Japanese: マージ後、親がキー不足なら再平衡化；根が子1つのみなら子を新根に、高さ -1。
    """
    # 根节点仅剩一子：将子提升为新根，树高 -1；旧根页归还 FSM
    if parent is tree._root and len(parent.keys) == 0:
        tree._root = parent.children[0]
        _try_free_discarded_node(tree, parent)
        return
    # 父欠键且非根：递归再平衡
    if len(parent.keys) < MIN_KEYS and parent is not tree._root and path:
        _rebalance(tree, parent, path)


def _insert_into_leaf(leaf: LeafNode, key: Any, value: Any) -> None:
    """
    English: Insert key-value into leaf, keeping keys sorted.
    Chinese: 向叶子节点插入 key-value，保持 keys 有序。
    Japanese: 葉ノードにキーと値を挿入し、キーをソート順に保持します。
    """
    idx = bisect.bisect_left(leaf.keys, key)
    if idx < len(leaf.keys) and leaf.keys[idx] == key:
        leaf.values[idx] = value
    else:
        leaf.keys.insert(idx, key)
        leaf.values.insert(idx, value)


def _split_leaf(leaf: LeafNode) -> tuple[Any, LeafNode]:
    """
    English: Split full leaf into two; B-Link: set right_sibling, high_key.
    Chinese: 分裂满载叶子；B-Link：设置 right_sibling、更新 high_key。
    Japanese: 満杯の葉を分割；B-Link：right_sibling と high_key を設定。

    Returns:
        (push_key, right_leaf) for parent to insert.
    """
    n = len(leaf.keys)
    mid = (n + 1) // 2

    right_leaf = LeafNode()
    right_leaf.keys = leaf.keys[mid:]
    right_leaf.values = leaf.values[mid:]

    push_key = right_leaf.keys[0]

    leaf.keys = leaf.keys[:mid]
    leaf.values = leaf.values[:mid]

    # B-Link: 左节点指向右兄弟（父更新前可沿链找到新区）
    leaf.right_sibling = right_leaf
    leaf.high_key = push_key if leaf.keys else None
    right_leaf.high_key = max(right_leaf.keys) if right_leaf.keys else None

    right_leaf.prev = leaf
    right_leaf.next = leaf.next
    leaf.next = right_leaf
    if right_leaf.next is not None:
        right_leaf.next.prev = right_leaf

    return push_key, right_leaf


def _split_internal(internal: InternalNode) -> tuple[Any, InternalNode]:
    """
    English: Split full internal node; B-Link: set right_sibling, high_key.
    Chinese: 分裂满载内部节点；B-Link：设置 right_sibling、更新 high_key。
    Japanese: 満杯の内部ノードを分割；B-Link：right_sibling と high_key を設定。

    Returns:
        (push_key, right_internal) for parent to insert.
    """
    n = len(internal.keys)
    mid = n // 2
    push_key = internal.keys[mid]

    right_internal = InternalNode()
    right_internal.keys = internal.keys[mid + 1 :]
    right_internal.children = internal.children[mid + 1 :]

    internal.keys = internal.keys[:mid]
    internal.children = internal.children[: mid + 1]

    # B-Link: 左节点指向右兄弟，更新 high_key 供父节点路由
    internal.right_sibling = right_internal
    internal.high_key = push_key if internal.keys else None
    right_internal.high_key = (
        max(right_internal.keys) if right_internal.keys else None
    )

    return push_key, right_internal
