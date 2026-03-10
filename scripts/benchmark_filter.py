#!/usr/bin/env python3
"""
Phase 27: 布隆过滤器点查加速基准测试。

English: Benchmark Bloom filter vs no-filter for point lookups (50% absent keys).
Chinese: 10,000 次随机点查（50% 不存在的 Key），对比开启/关闭布隆过滤器耗时。
"""

import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from bplus_tree.schema import Schema
from bplus_tree.table import RowTable

# 基准参数
INSERT_COUNT = 5000
QUERY_COUNT = 10_000
ABSENT_RATIO = 0.5  # 50% 查询的 key 不存在


def run_benchmark(enable_bloom: bool, keys_in: set[int], query_keys: list[int]) -> float:
    """创建表、插入数据、执行点查，返回总耗时（秒）。"""
    schema = Schema(fields=[("id", "INT"), ("v", "VARCHAR(32)")])
    table = RowTable(schema, primary_key="id", enable_bloom_filter=enable_bloom)
    for k in keys_in:
        table.insert_row([k, f"val_{k}"])
    start = time.perf_counter()
    for k in query_keys:
        table._point_lookup(k)
    return time.perf_counter() - start


def main() -> None:
    print("=" * 60)
    print("Phase 27: 布隆过滤器点查加速基准 / Bloom Filter Point Lookup Benchmark")
    print("=" * 60)
    print(f"插入 {INSERT_COUNT} 条，点查 {QUERY_COUNT} 次（{int(ABSENT_RATIO*100)}% 不存在的 Key）")
    print()

    random.seed(42)
    keys_in = set(random.sample(range(INSERT_COUNT * 4), INSERT_COUNT))
    absent_keys = [k for k in range(INSERT_COUNT * 4, INSERT_COUNT * 8) if k not in keys_in]
    query_keys = []
    for _ in range(QUERY_COUNT):
        if random.random() < ABSENT_RATIO:
            query_keys.append(random.choice(absent_keys))
        else:
            query_keys.append(random.choice(list(keys_in)))

    t_with = run_benchmark(True, keys_in, query_keys)
    t_without = run_benchmark(False, keys_in, query_keys)

    print(f"开启布隆过滤器:  {t_with*1000:.2f} ms")
    print(f"关闭布隆过滤器:  {t_without*1000:.2f} ms")
    if t_without > 0:
        speedup = t_without / t_with
        print(f"加速比: {speedup:.2f}x")
    print("=" * 60)


if __name__ == "__main__":
    main()
