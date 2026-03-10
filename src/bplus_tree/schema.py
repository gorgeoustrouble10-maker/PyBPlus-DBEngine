"""
模式管理：字段定义与行序列化。

English: Schema management for field definitions and row serialization.
Chinese: 模式管理：字段定义与行序列化。
Japanese: スキーマ管理：フィールド定義と行のシリアライズ。
"""

import struct
from typing import Any, Union

# 数据类型常量
TYPE_INT: str = "INT"
TYPE_FLOAT: str = "FLOAT"
TYPE_VARCHAR: str = "VARCHAR"

# 固定长度
INT_SIZE: int = 8
FLOAT_SIZE: int = 8
VARCHAR_LEN_PREFIX: int = 2  # 2 字节存储变长字符串实际长度


class Schema:
    """
    English: Defines field names and types; supports INT, FLOAT, VARCHAR(N).
    Chinese: 定义字段名与类型；支持 INT、FLOAT、VARCHAR(N)。
    Japanese: フィールド名と型を定義；INT、FLOAT、VARCHAR(N) をサポート。
    """

    def __init__(
        self,
        fields: list[tuple[str, str]] | None = None,
        varchar_lengths: dict[str, int] | None = None,
    ) -> None:
        """
        English: Create schema from field definitions.
        Chinese: 从字段定义创建模式。
        Japanese: フィールド定義からスキーマを作成します。

        Args:
            fields: [(name, type), ...] e.g. [("id", "INT"), ("name", "VARCHAR(32)")].
            varchar_lengths: Optional {field_name: max_len} for VARCHAR.
        """
        self._fields: list[tuple[str, str]] = fields or []
        self._varchar_lengths: dict[str, int] = varchar_lengths or {}
        for name, typ in self._fields:
            if typ.startswith("VARCHAR(") and typ.endswith(")"):
                n = int(typ[8:-1])
                self._varchar_lengths[name] = n

    def add_field(self, name: str, field_type: str) -> None:
        """
        English: Add a field; VARCHAR(N) parses N automatically.
        Chinese: 添加字段；VARCHAR(N) 自动解析 N。
        Japanese: フィールドを追加；VARCHAR(N) の N を自動解析します。
        """
        self._fields.append((name, field_type))
        if field_type.startswith("VARCHAR(") and field_type.endswith(")"):
            self._varchar_lengths[name] = int(field_type[8:-1])

    def serialize_row(self, values: list[Any]) -> bytes:
        """
        English: Serialize Python list to binary bytes per schema.
        Chinese: 根据模式将 Python 列表序列化为二进制字节。
        Japanese: スキーマに従い Python リストをバイナリバイトにシリアライズします。
        """
        if len(values) != len(self._fields):
            raise ValueError(
                f"Row has {len(values)} values, schema expects {len(self._fields)}"
            )
        chunks: list[bytes] = []
        for i, (name, typ) in enumerate(self._fields):
            val = values[i]
            if typ == TYPE_INT:
                iv = int(val) if val is not None else 0
                chunks.append(struct.pack("<q", iv))
            elif typ == TYPE_FLOAT:
                fv = float(val) if val is not None else 0.0
                chunks.append(struct.pack("<d", fv))
            elif typ.startswith(TYPE_VARCHAR):
                max_len = self._varchar_lengths.get(name, 255)
                s = str(val) if val is not None else ""
                raw = s.encode("utf-8")[:max_len]
                chunks.append(struct.pack("<H", len(raw)))
                chunks.append(raw)
            else:
                raise ValueError(f"Unknown type: {typ}")
        return b"".join(chunks)

    def deserialize_row(self, raw: bytes) -> list[Any]:
        """
        English: Deserialize binary bytes to Python list per schema.
        Chinese: 根据模式将二进制字节反序列化为 Python 列表。
        Japanese: スキーマに従いバイナリバイトを Python リストにデシリアライズします。
        """
        result: list[Any] = []
        off = 0
        for name, typ in self._fields:
            if typ == TYPE_INT:
                result.append(struct.unpack_from("<q", raw, off)[0])
                off += INT_SIZE
            elif typ == TYPE_FLOAT:
                result.append(struct.unpack_from("<d", raw, off)[0])
                off += FLOAT_SIZE
            elif typ.startswith(TYPE_VARCHAR):
                vlen = struct.unpack_from("<H", raw, off)[0]
                off += VARCHAR_LEN_PREFIX
                result.append(raw[off : off + vlen].decode("utf-8"))
                off += vlen
            else:
                raise ValueError(f"Unknown type: {typ}")
        return result

    def field_names(self) -> list[str]:
        """
        English: Return all field names.
        Chinese: 返回所有字段名。
        Japanese: 全フィールド名を返します。
        """
        return [f[0] for f in self._fields]

    def __len__(self) -> int:
        return len(self._fields)
