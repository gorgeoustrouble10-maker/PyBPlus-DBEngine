# PyBPlus-DBEngine 核心架构设计白皮书
# PyBPlus-DBEngine Core Architecture Design White Paper
# PyBPlus-DBEngine コアアーキテクチャ設計ホワイトペーパー

---

**Version**: 1.4  
**Date**: 2026  
**Status**: Phase 1–Phase 16 (Recovery & DDL)

---

## 目录 | Table of Contents | 目次

1. [Architecture Overview | 架构概览 | アーキテクチャ概要](#1-architecture-overview)
2. [B+ Tree Mechanics | B+ 树机制 | B+ 木メカニクス](#2-b-tree-mechanics)
3. [Persistence & Reliability | 持久化与可靠性 | 永続化と信頼性](#3-persistence--reliability)
4. [Concurrency & MVCC | 并发与多版本 | 並行性と MVCC](#4-concurrency--mvcc)
5. [Relational Layer | 关系层 | リレーショナルレイヤー](#5-relational-layer)
6. [Data Flow Diagram | 数据流图 | データフロー図](#6-data-flow-diagram)
7. [Performance Report | 性能报告 | パフォーマンスレポート](#7-performance-report)
8. [Phase 10: High Performance & Optimization | Phase 10：高级性能优化](#8-phase-10-high-performance--optimization)
9. [Phase 11-14: Logic Closure & Physical Optimization | Phase 11-14：逻辑闭环与物理优化](#9-phase-11-14-logic-closure--physical-optimization)
10. [Phase 15: Wire Protocol & SQL Execution | Phase 15：Wire Protocol 与 SQL 执行流程](#10-phase-15-wire-protocol--sql-execution)
11. [Phase 16: Recovery & DDL | Phase 16：恢复与数据定义语言](#11-phase-16-recovery--ddl)

---

## 1. Architecture Overview
## 架构概览
## アーキテクチャ概要

### 1.1 Page-based Storage
### 基于页的存储
### ページベースストレージ

| 項目 | Value | 说明 |
|------|-------|------|
| **PAGE_SIZE** | 4096 bytes | 4KB 固定页，与主流 OS 页面对齐 |
| **File Header** | 16 bytes | `B+DB` magic (4B) + root_page_id (4B) + next_page_id (4B) |
| **Page Layout** | offset = 16 + page_id × 4096 | 页在文件中的物理偏移 |

**Layout**: ファイルは「16バイトヘッダ + 4096バイト × N ページ」の構造を採用。各ページは B+ 木のノードに対応し、ディスク I/O の単位となる。

### 1.2 Buffer Pool Design
### 缓冲池设计
### バッファプール設計

```
┌─────────────────────────────────────────────────────────┐
│  LRU Cache (OrderedDict)                                │
│  page_id → (raw_bytes, is_dirty)                        │
│  max_pages = 64                                         │
└─────────────────────────────────────────────────────────┘
         │                    │
         ▼                    ▼
   get_page()           put_page()
   (read path)          (write path)
         │                    │
         │  miss: 从磁盘读取    │  dirty: 刷回时写入
         ▼                    ▼
   _read_page_from_disk   _write_page_to_disk
```

**Key mechanisms** / **核心机制** / **コアメカニズム**:
- **LRU eviction**: 最久未用页优先淘汰；脏页先刷回磁盘再淘汰
- **Atomic flush**: 先写临时文件 `.tmp`，`os.replace()` 原子覆盖，崩溃安全
- **Dirty tracking**: 修改过的页标记为 dirty，flush 时批量持久化

---

## 2. B+ Tree Mechanics
## B+ 树机制
## B+ 木メカニクス

### 2.1 Node Structure
### 节点结构
### ノード構造

| Node Type | keys | children / values | B-Link | 説明 |
|-----------|------|-------------------|--------|------|
| **InternalNode** | sorted keys | n+1 children | high_key, right_sibling | 内部ノード：キーによるルーティング |
| **LeafNode** | sorted keys | values (key-value pairs) | high_key, right_sibling | 葉ノード：双方向 next リンク |

**Order**: デフォルト order=4 → `MAX_KEYS=4`, `MIN_KEYS=2`。叶子溢出时触发分裂，欠键时触发再平衡。**B-Link**：各节点含 `high_key`（键上界）与 `right_sibling`（分裂产生的右兄弟链）。

### 2.2 Split Algorithm
### 分裂算法
### 分割アルゴリズム

**Leaf Split** (`_split_leaf`):
1. 中点 `mid = (n+1)//2`，左取 ceil(n/2) 个 key
2. 右叶子取 keys[mid:], values[mid:]
3. **Push key** = 右半最左 key（复制上提，保留于叶中，B+ 树规范）
4. 维护 prev/next 双向链表

**Internal Split** (`_split_internal`):
1. 中点 `mid = n//2`，`push_key = keys[mid]` 上提
2. 右内部节点取 keys[mid+1:], children[mid+1:]（中间键不再保留于当前层）
3. 左截断为 keys[:mid], children[:mid+1]

### 2.3 Merge & Rebalance
### 合并与再平衡
### マージとリバランス

**Borrow**: 优先向左/右兄弟借位（`len(sibling.keys) > MIN_KEYS`），避免合并。

**Merge**:
- 叶子：与左/右兄弟合并，更新父的 keys 与 children
- 内部节点：合并后若父欠键，递归 `_rebalance_parent`
- 根收缩：根仅剩一子时，子提升为新根，树高 -1

### 2.4 Latch Crabbing (Lock Crabbing)
### 锁螃蟹算法
### ラッチクラッピング

**Principle** / **原则** / **原理**:
- 向下遍历时，锁住当前节点，再释放祖先（仅当当前节点“安全”时）
- **Safe for insert**: `len(keys) < MAX_KEYS`，再插一个不会分裂
- **Safe for delete**: `len(keys) > MIN_KEYS`，再删一个不会合并

**Implementation**: `LatchManager` 为每节点维护 `RLock`；`CrabbingGuard` 在子节点安全时释放祖先锁。当前 BPlusTree 使用树级 `TreeLatch`（`read_guard` / `write_guard`）提供粗粒度并发，可扩展为细粒度 per-node crabbing。

---

## 3. Persistence & Reliability
## 持久化与可靠性
## 永続化と信頼性

### 3.1 Binary Page Layout
### 二进制页布局
### バイナリページレイアウト

**Internal Page**:
```
header(16): type(1)=0, key_count(2), parent_id(4)
keys(n×8): int64 little-endian
children((n+1)×4): int32 page_ids
[padding to 4096]
```

**Leaf Page (Legacy)**:
```
header(16): type(1)=1, key_count(2), parent_id(4)
keys(n×8): int64
prev_id(4), next_id(4): 叶链表
values: per value [len(2) + utf8_bytes]
[padding to 4096]
```

**Leaf Page (Slotted, Phase 12)**：
```
header(24): type(2), key_count, parent_id, prev_id, next_id, slot_array_end, free_start
SlotArray(4×n): 每 slot [offset(2), length(2)]
Free Space
Records(从页尾向页头): 每条 [key(8) + value_len(2) + value]
```
详见 `docs/SLOTTED_PAGE_LAYOUT.md`。

### 3.2 Superblock (Page 0)
### 超级块（页 0）
### スーパーブロック（ページ0）

持久化表使用 Page 0（4096 bytes）存储元数据：
- magic `DBT1`
- schema JSON、primary_key、order
- primary_root_id、secondary_index roots
- 其余为 JSON 形式的主/二级索引数据

### 3.3 WAL & Atomic Commit
### WAL 与原子提交
### WAL とアトミックコミット

**Format**:
```
TX_BEGIN <tx_id>
TX<tx_id> INSERT <key> <base64_value>
TX<tx_id> DELETE <key>
COMMIT <tx_id>
```

**Recovery**: 仅重放 **已 COMMIT** 的事务；未提交的 TX_BEGIN… 序列被丢弃，实现原子提交语义。

---

## 4. Concurrency & MVCC
## 并发与多版本
## 並行性と MVCC

### 4.1 Record Header
### 行头
### レコードヘッダ

每行前缀 20 字节：
- `RHD1` (4B) 魔数
- `transaction_id` (8B)
- `roll_pointer` (8B)，预留给 Undo Log

### 4.2 ReadView Visibility (深度解析)
### ReadView 可见性（深度解析）
### ReadView 可視性（詳細解析）

**问题**: 多版本并发下，某物理行的 `row_tx_id` 表示创建该行的事务。读者需要判断：是否应看到该版本？

**ReadView 三要素**:
- `creator_tx_id`: 创建该视图的读者事务 ID
- `committed`: 已提交事务 ID 集合
- `active_ids`: 快照时刻尚未提交的事务 ID 集合

**可见性判定**（`row_tx_id` 过滤机制）:

行对 ReadView 可见 ⟺ 以下三条同时满足：

1. **row_tx_id ∈ committed**  
   创建该行的事务必须已提交。

2. **row_tx_id ≤ creator_tx_id**  
   行的创建者不能晚于读者；读者不应看到“未来”数据。

3. **row_tx_id ∉ active_ids**  
   已提交则必不在 active；此条为双重保险。

**公式化**:
$$\text{visible}(row) \iff row\_tx\_id \in \text{committed} \,\wedge\, row\_tx\_id \leq creator\_tx\_id$$

---

## 5. Relational Layer
## 关系层
## リレーショナルレイヤー

### 5.1 Schema Serialization
### Schema 序列化
### スキーマシリアライズ

| 类型 | 格式 | サイズ |
|------|------|--------|
| INT | `struct.pack("<q", v)` | 8B |
| FLOAT | `struct.pack("<d", v)` | 8B |
| VARCHAR(N) | `len(2B) + utf8` | 2B + variable |

行格式：`[RHD1][tx_id 8B][roll_ptr 8B][payload]`

### 5.2 Secondary Index
### 二级索引
### セカンダリインデックス

- **Key**: `(field_value, primary_key)` 复合键，支持同字段多行
- **Value**: 占位 1（或主键引用）
- **插入/删除**: 主表变更时同步更新所有二级索引
- **get_by_index**: 有索引时 O(log N) 查找主键，再回表（Bookmark Lookup）取全行

---

## 6. Data Flow Diagram
## 数据流图
## データフロー図

以下流程图展示一条 INSERT 从 SQL 语义到写入 WAL、再更新 B+ 树的完整生命周期。

```mermaid
flowchart TD
    A["INSERT row<br/>(SQL 语义)"] --> B[RowTable.insert_row]

    B --> C[Tuple 构造]
    C --> D["Tuple.to_bytes(tx_id)"]
    D --> E[BPlusTree.insert]

    E --> F["① WAL.log_insert (先写日志)"]
    F --> G["② 自根向下查找目标叶子"]
    G --> H["③ 二分插入 key-value"]
    H --> I{叶子溢出?}
    I -->|是| J["④ 分裂 & 上提 push_key"]
    J --> K[递归至父节点]
    K --> I
    I -->|否| L[完成]

    H --> M["BufferPool / 内存节点"]
    M --> N["LRU 缓存 / flush 刷盘"]

    style F fill:#e1f5fe
    style H fill:#e8f5e9
    style J fill:#fff3e0
```

---

## 7. Performance Report
## 性能报告
## パフォーマンスレポート

### 7.1 Benchmark Setup
### 测试环境
### ベンチマーク環境

- 随机插入：100,000 条
- 范围查询：`[40_000, 49_999]` 约 10,000 条
- B+ 树 order=64（减小层数以优化插入）

### 7.2 Results
### 结果
### 結果

| 场景 / Scenario | BPlusTree | dict |
|-----------------|-----------|------|
| 随机插入 100k | ~870 ms | ~31 ms |
| 范围查询 10k | **~7 ms** | ~27 ms |

### 7.3 Analysis
### 分析
### 分析

**插入**: dict 在纯内存、无结构维护开销下更快；B+ 树需维护顺序与分裂，插入成本更高。

**范围查询**: B+ 树 **约 3.8× 更快**。
- B+ 树：沿叶子 `next` 链顺序扫描，**O(k)** 其中 k 为范围内记录数，无需全表过滤与排序
- dict：需全表遍历过滤 + 排序，**O(N log N)** 其中 N 为总记录数

**结论 / Conclusion / 結論**:  
B+ 树在范围查询场景下对原生 dict 具有明显优势，适合 OLAP 式批量扫描与区间检索，体现索引结构的核心价值。

---

## 8. Phase 10: High Performance & Optimization
## Phase 10：高级性能优化
## Phase 10：ハイパフォーマンスと最適化

### 8.1 B-Link Tree 雏形
### B-Link Tree 雏形
### B-Link Tree プロトタイプ

为 B+ 树节点引入 **high_key** 与 **right_sibling**，为高并发插入铺路。

| 属性 | 含义 |
|------|------|
| **high_key** | 该节点内键的上界；分裂后用于路由 |
| **right_sibling** | 分裂产生的右兄弟指针；父节点更新前可沿链找到新区 |

分裂时：左节点 `right_sibling = 右兄弟`，`high_key = push_key`；右节点 `high_key = max(keys)`。父节点插入后更新 `high_key = max(keys)`。

### 8.2 Free Space Map (FSM)
### 空闲空间映射
### 空き領域マップ

- **FreeSpaceMap**：用集合（逻辑位图）追踪已释放的 page_id
- **BufferPool.allocate_page**：优先从 FSM 复用空闲页，否则 `next_page_id++` 追加
- **BufferPool.free_page**：将页归还 FSM，供后续 allocate 复用

### 8.3 Cost-Based Optimizer (CBO-Lite)
### 代价优化器（轻量版）
### コストベースオプティマイザ（ライト版）

- **统计信息**：`_total_rows`（insert 时递增）、`_index_unique_counts`（预留）
- **choose_strategy(start_key, end_key)**：
  - 若 `scan_range / total_rows > 0.3` → `TABLE_SCAN`
  - 否则 → `INDEX_SCAN`
- **refresh_stats()**：从范围扫描重新计算 `total_rows`（如加载后）

---

## 9. Phase 11-14: Logic Closure & Physical Optimization
## Phase 11-14：逻辑闭环与物理优化
## Phase 11-14：ロジッククロージャと物理最適化

### 9.1 Phase 11：FSM 闭环与 CBO 联动
### Phase 11：FSM 闭环与 CBO 联动

- **FSM 闭环**：在 `tree.py` 的 merge 逻辑（`_merge_leaf_left/right`、`_merge_internal_left/right`）、根收缩、空树删除时，调用 `_try_free_discarded_node()`，将废弃页归还 `BufferPool.free_page()`。
- **load_from_db(keep_pool=True)**：树保留 pool 与 node_to_pid 映射，使 merge 时可正确释放页。
- **CBO 联动**：`scan_with_condition` 根据 `choose_strategy` 自动选择 `TABLE_SCAN` 或 `INDEX_SCAN`；`TABLE_SCAN` 时全表扫描并在条件中过滤用户 key 范围。

### 9.2 Phase 12：Slotted Page 物理布局
### Phase 12：Slotted Page 物理布局

- **布局**：Header(24B) | SlotArray | Free Space | Records（从页尾向页头生长）。
- **碎片整理**：`_compact_slotted_page()` 回收逻辑删除产生的空隙。
- **字节级定义**：见 `docs/SLOTTED_PAGE_LAYOUT.md`。

### 9.3 Phase 13：Undo Log 物理回滚
### Phase 13：Undo Log 物理回滚

- **UndoRecord**：`op`(INSERT/DELETE)、`key`、`before_image`、`after_image`。
- **流程**：`insert_row`/`delete_row` 在修改前调用 `transaction.log_insert_undo` / `log_delete_undo`。
- **rollback(tree)**：逆序应用 Undo 记录：INSERT 回滚 → delete(key)；DELETE 回滚 → insert(key, before_image)。
- **RowTable.rollback_transaction(tx)**：对表执行物理回滚。

### 9.4 Phase 14：后台刷脏与 Checkpoint
### Phase 14：后台刷脏与 Checkpoint

- **BackgroundWriter**：守护线程定时调用 `pool.flush_dirty_pages()` 批量刷脏。
- **do_checkpoint(pool, wal)**：刷写全部脏页，记录 `CHECKPOINT`。
- **truncate_wal_after_checkpoint(wal_path)**：截断 WAL，仅保留最后 CHECKPOINT 之后内容。

### 9.5 Undo/Redo 恢复逻辑
### Undo/Redo 恢复逻辑

| 机制 | 作用 |
|------|------|
| **WAL** | 修改前记录 INSERT/DELETE；崩溃后 Replay 重放已提交事务 |
| **Undo Log** | 修改前记录 before_image；回滚时逆序还原 Page 数据 |
| **Checkpoint** | 脏页刷盘后截断 WAL，缩短恢复时间 |

---

## 10. Phase 15: Wire Protocol & SQL Execution
## Phase 15：Wire Protocol 与 SQL 执行流程
## Phase 15：Wire Protocol と SQL 実行フロー

### 10.1 Wire Protocol
### 有线协议
### ワイヤプロトコル

| 方向 | 格式 | 说明 |
|------|------|------|
| **请求 Request** | `[4B length LE][UTF-8 SQL]` | length = payload 字节数 |
| **响应 Response** | `[4B length LE][UTF-8 text]` | 格式：`STATUS\nmessage\n[header\nrow1\n...]` |

- **STATUS**：`OK` 或 `ERROR`
- **message**：状态描述（如 `(3 rows)` 或错误信息）
- **header/row**：SELECT 时，表头与行均为 TAB 分隔

### 10.2 SQL 解析器 (Parser Lite)
### SQL Parser Lite

- **支持语句**：`SELECT * FROM t [WHERE id>=x AND id<=y]`、`INSERT INTO t (c1,c2) VALUES (v1,v2)`、`DELETE FROM t WHERE id=v`
- **实现**：`sql_engine.py`，正则与字符串切分，输出 `ParsedSelect` / `ParsedInsert` / `ParsedDelete`
- **映射**：`execute_sql()` 调用 `RowTable.scan_with_condition` / `insert_row` / `delete_row`

### 10.3 多线程 TCP 服务器
### 多线程 TCP サーバー

- **实现**：`socketserver.ThreadingTCPServer`，每连接一个 `DBRequestHandler` 线程
- **每连接独立事务**：`BEGIN` 开启、`COMMIT` 提交、`ROLLBACK` 物理回滚
- **启动**：`scripts/run_server.py -H 127.0.0.1 -P 8765`

### 10.4 交互式 CLI 客户端
### 対話型 CLI クライアント

- **脚本**：`scripts/cli_client.py -H host -P port`
- **用法**：类似 `mysql -u root`，输入 SQL，表格形式输出
- **命令**：`QUIT` / `EXIT` 断开连接

### 10.5 SQL 执行流程
### SQL 実行フロー

```
Client                    Server                     RowTable
   |                         |                            |
   |--[Length][SQL]--------->|                            |
   |                         |-- parse_sql(sql)           |
   |                         |-- execute_sql(table,tx)    |
   |                         |   |-- insert_row /         |
   |                         |   |   scan_with_condition/ |
   |                         |   |   delete_row --------->|
   |                         |<-- (msg, rows, cols) ------|
   |<-[Length][OK\n...]------|                            |
```

---

## 11. Phase 16: Recovery & DDL
## Phase 16：恢复与数据定义语言
## Phase 16：リカバリと DDL

### 11.1 Recovery Algorithm | 恢复算法 | リカバリアルゴリズム

- **目标**：服务器崩溃后，启动时自动重放 WAL，使内存与磁盘保持一致。
- **触发**：`run_server_with_recovery(data_dir)` 时，若存在 `wal.log` 或 `wal_{table}.log`，则执行恢复。
- **流程**：`DatabaseContext.run_recovery()` → 按表查找 `wal_{name}.log` → 调用 `WAL.replay()` → 仅重放状态为 `COMMIT` 的事务中的物理操作（`INSERT`/`DELETE`）。
- **兼容**：无 Catalog 时，重放全局 `wal.log` 到第一个表；有 Catalog 时，每个表使用各自的 `wal_{name}.log`。

| 步骤 | 说明 |
|------|------|
| 1. 加载 Catalog | 从 `catalog.json` 读取表列表与 Schema |
| 2. 加载表 | 为每个表创建 `RowTable`，打开对应数据文件 |
| 3. WAL 重放 | 对每个表重放 `wal_{name}.log`（若存在） |
| 4. 启动服务 | 进入正常请求处理循环 |

### 11.2 DDL Handling | DDL 处理 | DDL 処理

- **CREATE TABLE**：`CREATE TABLE name (col1 INT, col2 VARCHAR(32), ...)`，支持 `PRIMARY KEY (col)` 或 `col INT PRIMARY KEY`。
- **实现**：`sql_engine.py` 中 `_parse_create_table()` 解析列定义，`execute_sql()` 在传入 `DatabaseContext` 时调用 `db.create_table()`。
- **动态创建**：解析后创建 `RowTable`，写入 `catalog.json`，并为该表创建专用 `wal_{name}.log`。

| 语法 | 示例 |
|------|------|
| 列定义 | `id INT`, `name VARCHAR(32)` |
| 主键（列级） | `id INT PRIMARY KEY` |
| 主键（表级） | `PRIMARY KEY (id)` |

### 11.3 Metadata Persistence | 元数据持久化 | メタデータ永続化

- **存储**：`catalog.json` 保存 `{ table_name: { "fields": [...], "primary_key": "col" } }`。
- **加载**：`Catalog.load()` 在 `DatabaseContext` 初始化时执行，`load_tables()` 根据 Catalog 重建所有表。
- **效果**：服务器重启后，之前创建的表依然存在，数据可通过 `SELECT` 查询。

### 11.4 DBError & Robustness | 错误码与鲁棒性 | エラーコードとロバスト性

- **体系**：`errors.py` 中定义 `DBError` 基类，包含 `code`、`message`、`format_for_wire()`。
- **常用错误码**：`1064` 语法错误、`1065` 空 SQL、`1146` 未知表、`1050` 表已存在、`1054` 未知列、`1062` 重复键、`1032` 键不存在、`1049` 未知数据库、`1109` 不支持语句。
- **保护**：Server 捕获 `DBError`，通过 `format_for_wire()` 返回 `ERROR\n<code> <message>\n`，避免非法输入导致进程崩溃。

### 11.5 启动方式 | Startup Modes | 起動モード

| 模式 | 命令 | 说明 |
|------|------|------|
| 单表模式 | `run_server.py -T mytable` | 兼容旧版，无恢复、无 Catalog |
| 恢复模式 | `run_server.py -d ./data` | 加载 Catalog、执行 WAL 恢复、支持 DDL |

---

## References
## 参考文献
## 参考文献

- PyBPlus-DBEngine 源码：`src/bplus_tree/`
- 基准脚本：`scripts/benchmark.py`
- 测试：`tests/test_*.py`

---

*Document generated from Phase 1–Phase 16 implementation.  
本白皮书基于 Phase 1 至 Phase 16 的完整实现生成。  
Phase 1〜Phase 16 の実装に基づいて本ホワイトペーパーを生成しました。*

---

### 后续迭代建议 | Next Iteration Suggestions | 今後のイテレーション提案

| 优先级 | 方向 | 说明 |
|--------|------|------|
| **P1** | 多表事务回滚 | `ROLLBACK` 需作用于所有已修改表，而非仅主表 |
| **P1** | DROP TABLE | 实现 `DROP TABLE name`，删除表文件并更新 Catalog |
| **P2** | ALTER TABLE | 支持 `ADD COLUMN`、`DROP COLUMN`，需更新 Schema 与 Catalog |
| **P2** | 复杂主键 | 支持多列 `PRIMARY KEY (a, b)` |
| **P2** | Checkpoint 优化 | 脏页刷盘后截断 WAL，缩短恢复时间 |
| **P3** | 多数据库 | `USE database`、`CREATE DATABASE`，Catalog 按库分目录 |
| **P3** | 索引 DDL | `CREATE INDEX`、`DROP INDEX`，独立索引文件管理 |
