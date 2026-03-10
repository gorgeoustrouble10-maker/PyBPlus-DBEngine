"""
Schema 与 Table（Tuple、RowTable）测试。
"""

import pytest

from bplus_tree.schema import Schema
from bplus_tree.table import RowTable, Tuple


class TestSchema:
    """Schema 序列化/反序列化测试。"""

    def test_add_field_and_serialize_int_float_varchar(self) -> None:
        """INT、FLOAT、VARCHAR 可正确序列化与反序列化。"""
        schema = Schema()
        schema.add_field("id", "INT")
        schema.add_field("name", "VARCHAR(32)")
        schema.add_field("score", "FLOAT")

        row = [1, "Alice", 95.5]
        raw = schema.serialize_row(row)
        assert isinstance(raw, bytes)
        assert len(raw) > 0

        decoded = schema.deserialize_row(raw)
        assert decoded[0] == 1
        assert decoded[1] == "Alice"
        assert decoded[2] == pytest.approx(95.5)

    def test_schema_from_fields(self) -> None:
        """从 fields 列表构造 Schema。"""
        schema = Schema(
            fields=[
                ("id", "INT"),
                ("name", "VARCHAR(64)"),
                ("value", "FLOAT"),
            ]
        )
        assert schema.field_names() == ["id", "name", "value"]
        assert len(schema) == 3

    def test_field_names_and_len(self) -> None:
        """field_names 与 __len__ 正确。"""
        schema = Schema(fields=[("a", "INT"), ("b", "FLOAT")])
        assert schema.field_names() == ["a", "b"]
        assert len(schema) == 2

    def test_serialize_wrong_length_raises(self) -> None:
        """行长度与 schema 不匹配时抛出 ValueError。"""
        schema = Schema(fields=[("id", "INT"), ("x", "FLOAT")])
        with pytest.raises(ValueError, match="expects"):
            schema.serialize_row([1])  # 少一个
        with pytest.raises(ValueError, match="expects"):
            schema.serialize_row([1, 2.0, "extra"])  # 多一个


class TestTuple:
    """Tuple 类测试。"""

    def test_tuple_from_values(self) -> None:
        """从 values 列表创建 Tuple。"""
        schema = Schema(fields=[("id", "INT"), ("name", "VARCHAR(16)")])
        t = Tuple(schema, values=[42, "Bob"])
        assert t.get_field("id") == 42
        assert t.get_field("name") == "Bob"

    def test_tuple_from_raw(self) -> None:
        """从 raw bytes 创建 Tuple。"""
        schema = Schema(fields=[("id", "INT"), ("score", "FLOAT")])
        raw = schema.serialize_row([7, 88.5])
        t = Tuple(schema, raw=raw)
        assert t.get_field("id") == 7
        assert t.get_field("score") == pytest.approx(88.5)

    def test_tuple_get_field_raises_on_unknown(self) -> None:
        """get_field 对不存在的字段名抛出 KeyError。"""
        schema = Schema(fields=[("id", "INT")])
        t = Tuple(schema, values=[1])
        with pytest.raises(KeyError, match="foo"):
            t.get_field("foo")

    def test_tuple_to_bytes_as_list_as_dict(self) -> None:
        """to_bytes、as_list、as_dict 正确。"""
        schema = Schema(fields=[("id", "INT"), ("name", "VARCHAR(8)")])
        t = Tuple(schema, values=[3, "Hi"])
        raw = t.to_bytes()
        payload = schema.serialize_row([3, "Hi"])
        assert raw == b"RHD1" + b"\x01\x00\x00\x00\x00\x00\x00\x00" + b"\x00\x00\x00\x00\x00\x00\x00\x00" + payload
        assert t.as_list() == [3, "Hi"]
        assert t.as_dict() == {"id": 3, "name": "Hi"}


class TestRowTable:
    """RowTable 与 insert_row、scan_with_condition 测试。"""

    def test_insert_row_and_scan_all(self) -> None:
        """insert_row 插入后 scan_with_condition(恒真) 可扫出所有行。"""
        schema = Schema()
        schema.add_field("id", "INT")
        schema.add_field("name", "VARCHAR(32)")
        schema.add_field("score", "FLOAT")

        table = RowTable(schema, primary_key="id")
        table.insert_row([1, "Alice", 95.5])
        table.insert_row([2, "Bob", 88.0])
        table.insert_row([3, "Carol", 92.3])

        rows = list(table.scan_with_condition(lambda r: True))
        assert len(rows) == 3
        names = [r.get_field("name") for r in rows]
        assert "Alice" in names and "Bob" in names and "Carol" in names

    def test_scan_with_condition_filter(self) -> None:
        """scan_with_condition 按条件过滤。"""
        schema = Schema()
        schema.add_field("id", "INT")
        schema.add_field("score", "FLOAT")

        table = RowTable(schema, primary_key="id")
        for i in range(1, 6):
            table.insert_row([i, 70.0 + i * 5.0])  # 75, 80, 85, 90, 95

        high_scores = list(
            table.scan_with_condition(lambda r: r.get_field("score") >= 85.0)
        )
        assert len(high_scores) == 3
        scores = [r.get_field("score") for r in high_scores]
        assert all(s >= 85.0 for s in scores)

    def test_scan_with_key_range(self) -> None:
        """scan_with_condition 支持 start_key、end_key 范围。"""
        schema = Schema()
        schema.add_field("id", "INT")
        schema.add_field("x", "FLOAT")

        table = RowTable(schema, primary_key="id")
        for i in range(1, 11):
            table.insert_row([i, float(i)])

        rows = list(
            table.scan_with_condition(
                lambda r: True, start_key=3, end_key=7
            )
        )
        ids = sorted(r.get_field("id") for r in rows)
        assert ids == [3, 4, 5, 6, 7]

    def test_primary_key_not_in_schema_raises(self) -> None:
        """主键不在 schema 中时抛出 ValueError。"""
        schema = Schema(fields=[("id", "INT")])
        with pytest.raises(ValueError, match="not in schema"):
            RowTable(schema, primary_key="nonexistent")

    def test_insert_row_wrong_length_raises(self) -> None:
        """insert_row 行长度不匹配时抛出 ValueError。"""
        schema = Schema(fields=[("id", "INT"), ("name", "VARCHAR(8)")])
        table = RowTable(schema, primary_key="id")
        with pytest.raises(ValueError, match="values"):
            table.insert_row([1])  # 少一个

    def test_choose_strategy_cbo_lite(self) -> None:
        """choose_strategy：扫描范围 > 30% 返回 TABLE_SCAN，否则 INDEX_SCAN。"""
        schema = Schema(fields=[("id", "INT"), ("x", "FLOAT")])
        table = RowTable(schema, primary_key="id")
        for i in range(100):
            table.insert_row([i, float(i)])
        assert table.choose_strategy(0, 29) == "INDEX_SCAN"
        assert table.choose_strategy(0, 39) == "TABLE_SCAN"
