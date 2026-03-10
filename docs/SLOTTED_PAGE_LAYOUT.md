# Slotted Page 物理布局定义
# Slotted Page Physical Layout Definition
# スロット付きページ物理レイアウト定義

---

**Version**: 1.0  
**Date**: 2026  
**Purpose**: Phase 12 字节级实现依据

---

## 1. 页全局常量

| 常量 | 值 | 说明 |
|------|-----|------|
| PAGE_SIZE | 4096 | 每页固定 4KB |
| SLOT_SIZE | 4 | 每个 slot 占用 4 字节 (offset 2B + length 2B) |
| MAX_SLOTS | 512 | 每页最多 512 条记录 (SlotArray 最大 2048B) |

---

## 2. LeafPage 布局（从偏移 0 到 PAGE_SIZE-1）

### 2.1 布局总览

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ 偏移 0                                                            PAGE_SIZE  │
├─────────────────────────────────────────────────────────────────────────────┤
│ Header (固定 24 字节)                                                         │
├─────────────────────────────────────────────────────────────────────────────┤
│ SlotArray (变长，从 Header 后向页尾生长)                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│ Free Space (中间可变区域)                                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│ Records (从页尾向页头生长，变长 record)                                       │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Header 字节定义（共 24 字节）

| 偏移 | 长度 | 类型 | 字段名 | 说明 |
|------|------|------|--------|------|
| 0 | 1 | uint8 | page_type | 0=Internal, 1=Leaf |
| 1 | 2 | uint16 | key_count | 当前 slot 数量（有效记录数） |
| 3 | 4 | int32 | parent_id | 父节点 page_id，-1 表示根 |
| 7 | 4 | int32 | prev_id | 叶链表前驱，-1 无 |
| 11 | 4 | int32 | next_id | 叶链表后继，-1 无 |
| 15 | 2 | uint16 | slot_array_end | SlotArray 结束偏移（从页头起算） |
| 17 | 2 | uint16 | free_start | 空闲区起始偏移（记录区向页头方向的边界） |
| 19 | 2 | uint16 | reserved | 保留，填 0 |
| 21 | 2 | uint16 | checksum | 可选校验和，Phase 12 暂填 0 |
| 23 | 1 | uint8 | padding | 对齐用 |

**struct.pack 格式**: `"<BHi i i HHxxHx"`  
- B: page_type  
- H: key_count  
- i: parent_id  
- i: prev_id (需要单独，因前有 3B)  
- 实际上应连续: `<B Hi i i HH H x`  
- 简化: 24 字节分块 pack，避免错误

**推荐分块**:
- Block1: `struct.pack("<B", page_type)` → 1B
- Block2: `struct.pack("<H", key_count)` → 2B  
- Block3: `struct.pack("<i", parent_id)` → 4B
- Block4: `struct.pack("<i", prev_id)` → 4B
- Block5: `struct.pack("<i", next_id)` → 4B
- Block6: `struct.pack("<H", slot_array_end)` → 2B
- Block7: `struct.pack("<H", free_start)` → 2B
- Block8: 5B padding

### 2.3 SlotArray 字节定义（每 slot 4 字节）

| 偏移 (相对 SlotArray 起始) | 长度 | 类型 | 字段 | 说明 |
|---------------------------|------|------|------|------|
| i*4 + 0 | 2 | uint16 | offset | 该记录在页内的起始偏移（从页头起算） |
| i*4 + 2 | 2 | uint16 | length | 该记录字节长度 |

- SlotArray 起始偏移 = 24 (HEADER_SIZE)
- SlotArray 向页尾生长：slot 0 在 24-27，slot 1 在 28-31，...
- slot_array_end = 24 + key_count * 4

**struct 格式（单 slot）**: `"<HH"` → offset, length

### 2.4 Record 格式（变长，从页尾向前生长）

每条 record 存储一个 key-value 对：

| 部分 | 长度 | 类型 | 说明 |
|------|------|------|------|
| key | 8 | int64 | B+ 树键 |
| value_len | 2 | uint16 | value 字节长度 |
| value | value_len | bytes | UTF-8 编码的 value |

**struct 格式（record 头）**: `"<qH"` → key, value_len

- Record 总长 = 8 + 2 + value_len = 10 + value_len
- Records 从 PAGE_SIZE-1 向页头方向写，每条 record 的 offset 指向 record 的起始位置

### 2.5 空闲空间与指针语义

- **slot_array_end**: 最后一个 slot 之后的下一个字节偏移。初始 = 24。
- **free_start**: 记录区向页头方向的边界。初始 = PAGE_SIZE。插入新 record 时，free_start 减小。
- **可用空闲** = free_start - slot_array_end（字节数）
- **插入流程**:
  1. 计算 record 需 r 字节
  2. 若 free_start - r < slot_array_end + 4，需要 compact 或分裂
  3. free_start -= r，在 free_start 处写入 record
  4. 在 SlotArray 追加 (offset=free_start, length=r)
  5. key_count += 1

### 2.6 删除与 _compact_page

- **删除**: 将 slot 标记为无效（offset=0xFFFF, length=0xFFFF），或从 SlotArray 移除并 compact。
- **Phase 12 简化**: 删除时将 slot 置为 (0xFFFF, 0)，逻辑删除。compact 时跳过无效 slot，重排有效 record。

**_compact_page 流程**:
1. 收集所有有效 (offset, length) 的 record 内容
2. 从页尾向前重新写入，更新每个 slot 的 offset/length
3. 更新 free_start = 最后一个 record 的 offset

---

## 3. InternalPage 布局（保持不变）

Internal 页不采用 Slotted 布局，保持现有固定格式：

| 偏移 | 长度 | 内容 |
|------|------|------|
| 0 | 16 | type(1)+key_count(2)+parent_id(4)+padding(9) |
| 16 | n*8 | keys |
| 16+n*8 | (n+1)*4 | children page_ids |

---

## 4. struct 格式汇总（防错检查）

| 用途 | 格式 | 长度 |
|------|------|------|
| Leaf Header | 分 8 次 pack 或 `<BHiHH` + padding | 24 |
| Slot 条目 | `<HH` | 4 |
| Record 头 | `<qH` | 10 |
| Key (单独) | `<q` | 8 |
| Value 长度 | `<H` | 2 |

**切勿使用** 模糊的 `f"<{n}q"` 与变长混在同一 pack 中；应逐块 pack 后 b''.join。
