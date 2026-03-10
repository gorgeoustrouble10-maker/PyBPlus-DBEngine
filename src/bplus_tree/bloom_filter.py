"""
Phase 27: 内存布隆过滤器；点查加速。

English: In-memory Bloom filter for point lookup optimization.
Chinese: 内存布隆过滤器；点查加速。
"""

import hashlib
from typing import Any


class BloomFilter:
    """
    English: Bloom filter using bit array; may_add before search to skip IO.
    Chinese: 基于位数组的布隆过滤器；search 前咨询可跳过昂贵 IO。
    """

    def __init__(self, num_bits: int = 8192, num_hashes: int = 3) -> None:
        """
        Args:
            num_bits: Bit array size (default 8192 = 1KB).
            num_hashes: Number of hash functions (k).
        """
        self._num_bits = num_bits
        self._num_hashes = num_hashes
        self._bits = bytearray((num_bits + 7) // 8)

    def _hashes(self, key: Any) -> list[int]:
        """Compute k hash positions for key."""
        raw = str(key).encode("utf-8")
        h = hashlib.sha256(raw).digest()
        positions: list[int] = []
        for i in range(self._num_hashes):
            # Use different slices of hash for each "function"
            start = (i * 8) % (len(h) - 4)
            val = int.from_bytes(h[start : start + 4], "big")
            positions.append(val % self._num_bits)
        return positions

    def add(self, key: Any) -> None:
        """Add key to filter."""
        for pos in self._hashes(key):
            byte_idx = pos // 8
            bit_idx = pos % 8
            self._bits[byte_idx] |= 1 << bit_idx

    def may_contain(self, key: Any) -> bool:
        """
        Return True if key might be present; False means definitely not present.
        False negatives: never. False positives: possible.
        """
        for pos in self._hashes(key):
            byte_idx = pos // 8
            bit_idx = pos % 8
            if not (self._bits[byte_idx] & (1 << bit_idx)):
                return False
        return True
