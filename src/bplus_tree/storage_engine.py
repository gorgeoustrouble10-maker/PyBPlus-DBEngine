"""
Phase 27: 存储引擎抽象层；可插拔架构。

English: Storage engine abstraction; pluggable architecture.
Chinese: 存储引擎抽象层；可插拔架构。
"""

from typing import Any, Iterator, Optional, Protocol, runtime_checkable


@runtime_checkable
class StorageEngine(Protocol):
    """
    English: Protocol for pluggable storage engines; search, insert, delete, scan.
    Chinese: 可插拔存储引擎协议；search、insert、delete、scan。
    """

    def search(self, key: Any) -> Optional[Any]:
        """Point lookup; return value or None."""
        ...

    def insert(self, key: Any, value: Any) -> None:
        """Insert key-value pair."""
        ...

    def delete(self, key: Any) -> None:
        """Delete by key; raise KeyError if not found."""
        ...

    def range_scan(self, start_key: Any, end_key: Any) -> Iterator[tuple[Any, Any]]:
        """Range scan; yield (key, value) in [start_key, end_key]."""
        ...


class BPlusTreeEngine:
    """
    English: StorageEngine implementation wrapping BPlusTree.
    Chinese: 包装 BPlusTree 的 StorageEngine 实现。
    """

    def __init__(self, tree: Any) -> None:
        self._tree = tree

    def search(self, key: Any) -> Optional[Any]:
        return self._tree.search(key)

    def insert(self, key: Any, value: Any) -> None:
        self._tree.insert(key, value)

    def delete(self, key: Any) -> None:
        self._tree.delete(key)

    def range_scan(self, start_key: Any, end_key: Any) -> Iterator[tuple[Any, Any]]:
        yield from self._tree.range_scan(start_key, end_key)

    @property
    def tree(self) -> Any:
        """Underlying BPlusTree for persistence/replication compatibility."""
        return self._tree
