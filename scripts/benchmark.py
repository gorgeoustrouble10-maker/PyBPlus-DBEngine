#!/usr/bin/env python3
"""
PyBPlus-DBEngine 性能压测脚本。

English: Performance benchmark comparing BPlusTree vs dict.
Chinese: 性能压测：对比 BPlusTree 与 dict。
Japanese: パフォーマンスベンチマーク：BPlusTree と dict の比較。
"""

import logging
import random
import sys
import time
from pathlib import Path

# 添加项目根目录以导入 bplus_tree
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from bplus_tree.tree import BPlusTree

# 压测参数
INSERT_COUNT = 100_000
RANGE_SCAN_COUNT = 10_000
RANGE_SIZE = 10_000  # 范围查询时选取的 key 范围大小


def _format_time(seconds: float) -> str:
    """格式化时间（秒 -> 毫秒/秒）。"""
    if seconds < 0.001:
        return f"{seconds * 1_000_000:.2f} μs"
    if seconds < 1:
        return f"{seconds * 1000:.2f} ms"
    return f"{seconds:.2f} s"


def bench_bplustree_insert(keys: list[int]) -> float:
    """B+ 树随机插入 10 万条。"""
    tree = BPlusTree(order=64)  # 较大阶数以减少层数
    start = time.perf_counter()
    for i, k in enumerate(keys):
        tree.insert(k, f"v_{k}")
    return time.perf_counter() - start


def bench_dict_insert(keys: list[int]) -> float:
    """dict 随机插入 10 万条。"""
    d: dict[int, str] = {}
    start = time.perf_counter()
    for k in keys:
        d[k] = f"v_{k}"
    return time.perf_counter() - start


def bench_bplustree_range_scan(tree: BPlusTree, start_key: int, end_key: int) -> float:
    """B+ 树范围查询 1 万条（沿叶子链顺序扫描）。"""
    start = time.perf_counter()
    count = 0
    for _ in tree.range_scan(start_key, end_key):
        count += 1
    return time.perf_counter() - start


def bench_dict_range_scan(d: dict[int, str], start_key: int, end_key: int) -> float:
    """dict 范围查询：过滤后排序（模拟范围扫描）。"""
    start = time.perf_counter()
    items = [(k, v) for k, v in d.items() if start_key <= k <= end_key]
    items.sort(key=lambda x: x[0])
    count = len(items)
    return time.perf_counter() - start


def main() -> None:
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(message)s")
    log = logging.info
    log("=" * 60)
    log("PyBPlus-DBEngine 性能压测 / Performance Benchmark")
    log("=" * 60)

    # 1. 生成随机 key 序列
    random.seed(42)
    keys = list(range(INSERT_COUNT))
    random.shuffle(keys)

    # 2. 随机插入 10 万条
    log("\n[1] 随机插入 (Random Insert) 100,000 条")
    t_bp_insert = bench_bplustree_insert(keys)
    t_dict_insert = bench_dict_insert(keys)
    log("    BPlusTree: %s", _format_time(t_bp_insert))
    log("    dict:     %s", _format_time(t_dict_insert))

    # 3. 重建结构供范围查询
    tree = BPlusTree(order=64)
    d: dict[int, str] = {}
    for k in keys:
        tree.insert(k, f"v_{k}")
        d[k] = f"v_{k}"

    # 4. 范围查询 1 万条（选取中间段 [40000, 50000)）
    start_key, end_key = 40_000, 49_999
    print(f"\n[2] 顺序范围查询 (Range Scan) [{start_key}, {end_key}]")
    t_bp_range = bench_bplustree_range_scan(tree, start_key, end_key)
    t_dict_range = bench_dict_range_scan(d, start_key, end_key)
    print(f"    BPlusTree: {_format_time(t_bp_range)}  ← 沿叶子链顺序扫描")
    print(f"    dict:     {_format_time(t_dict_range)}  ← 过滤 + 排序")

    # 5. 格式化对比表
    def _cell(s: str, w: int = 16) -> str:
        return (" " + s).ljust(w)

    bp_ins = _cell(_format_time(t_bp_insert))
    d_ins = _cell(_format_time(t_dict_insert))
    bp_rng = _cell(_format_time(t_bp_range))
    d_rng = _cell(_format_time(t_dict_range))
    log("\n" + "=" * 60)
    log("对比结果 (Comparison Results)")
    log("=" * 60)
    tbl = f"""
+------------------+------------------+------------------+
| 场景 / Scenario  | BPlusTree        | dict             |
+------------------+------------------+------------------+
| 随机插入 100k    | {bp_ins}| {d_ins}|
| 范围查询 10k     | {bp_rng}| {d_rng}|
+------------------+------------------+------------------+
"""
    log("%s", tbl)
    if t_dict_range > 0:
        ratio = t_dict_range / t_bp_range
        log("★ 范围查询优势: B+ 树约为 dict 的 %.1fx 快", ratio)
        log("  (B+ 树沿叶子 next 链顺序扫描，无需全表过滤与排序)")
    log("=" * 60)


if __name__ == "__main__":
    main()
