"""
Phase 15 测试：SQL 解析器、执行引擎、Wire Protocol。
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from bplus_tree.schema import Schema
from bplus_tree.table import RowTable
from bplus_tree.sql_engine import parse_sql, execute_sql
from bplus_tree.server import run_server, MSG_HEADER_LEN, _encode_response_correct


class TestSQLParser:
    """SQL 解析器测试。"""

    def test_parse_select(self) -> None:
        p = parse_sql("SELECT * FROM t")
        assert p.table == "t"
        assert p.columns == ["*"]

    def test_parse_select_with_where(self) -> None:
        p = parse_sql("SELECT * FROM t WHERE id >= 1 AND id <= 10")
        assert p.start_key == 1
        assert p.end_key == 10

    def test_parse_insert(self) -> None:
        p = parse_sql("INSERT INTO t (id, name) VALUES (1, 'alice')")
        assert p.table == "t"
        assert p.columns == ["id", "name"]
        assert p.values == [1, "alice"]

    def test_parse_insert_values_only(self) -> None:
        p = parse_sql("INSERT INTO t VALUES (2, 'bob', 3.14)")
        assert p.table == "t"
        assert p.values == [2, "bob", 3.14]

    def test_parse_delete(self) -> None:
        p = parse_sql("DELETE FROM t WHERE id = 5")
        assert p.table == "t"
        assert p.pk_value == 5


class TestSQLExecute:
    """SQL 执行引擎测试。"""

    def test_execute_insert_select_delete(self) -> None:
        schema = Schema(fields=[("id", "INT"), ("name", "VARCHAR(32)")])
        table = RowTable(schema, primary_key="id")

        msg, rows, cols = execute_sql("INSERT INTO t (id, name) VALUES (1, 'a')", table)
        assert msg == "INSERT ok"
        assert cols is None

        msg, rows, cols = execute_sql("SELECT * FROM t", table)
        assert "(1 rows)" in msg
        assert cols == ["id", "name"]
        assert rows == [[1, "a"]]

        msg, _, _ = execute_sql("DELETE FROM t WHERE id = 1", table)
        assert msg == "DELETE ok"

        msg, rows, _ = execute_sql("SELECT * FROM t", table)
        assert len(rows) == 0


class TestCreateTable:
    """CREATE TABLE 解析与执行测试。"""

    def test_parse_create_table(self) -> None:
        from bplus_tree.sql_engine import parse_sql
        p = parse_sql("CREATE TABLE users (id INT, name VARCHAR(32))")
        assert p.table == "users"
        assert p.columns == [("id", "INT"), ("name", "VARCHAR(32)")]
        assert p.primary_key == "id"

    def test_execute_create_table(self) -> None:
        import tempfile
        from pathlib import Path
        from bplus_tree.database_context import DatabaseContext
        from bplus_tree.sql_engine import execute_sql

        with tempfile.TemporaryDirectory() as d:
            ctx = DatabaseContext(Path(d))
            msg, _, _ = execute_sql(
                "CREATE TABLE t1 (id INT, x FLOAT)",
                db=ctx,
            )
            assert "CREATE TABLE ok" in msg
            t = ctx.get_table("t1")
            assert t is not None

    def test_dberror_codes(self) -> None:
        from bplus_tree.errors import SQLSyntaxError, UnknownTableError
        e = SQLSyntaxError("bad sql")
        assert e.code == 1064
        e2 = UnknownTableError("t")
        assert e2.code == 1146
        assert "[1146]" in e2.format_for_wire()


class TestPhase17:
    """Phase 17: 多表事务回滚、DROP TABLE 持久化。"""

    def test_global_rollback(self) -> None:
        """事务修改 Table A 后修改 Table B 失败，验证 A 是否回滚。"""
        import tempfile
        from pathlib import Path
        from bplus_tree.database_context import DatabaseContext
        from bplus_tree.transaction import TransactionManager
        from bplus_tree.sql_engine import execute_sql

        with tempfile.TemporaryDirectory() as d:
            ctx = DatabaseContext(Path(d))
            execute_sql("CREATE TABLE a (id INT, x VARCHAR(8))", db=ctx)
            execute_sql("CREATE TABLE b (id INT, y VARCHAR(8))", db=ctx)
            tx_mgr = TransactionManager()
            tx = tx_mgr.begin()

            execute_sql("INSERT INTO a (id, x) VALUES (1, 'v')", db=ctx, tx=tx)
            rows_a = list(ctx.get_table("a").scan_with_condition(lambda _: True))
            assert len(rows_a) == 1

            try:
                execute_sql(
                    "DELETE FROM b WHERE id = 999",
                    db=ctx,
                    tx=tx,
                )
            except KeyError:
                pass

            tx.rollback()
            tx_mgr.abort(tx)

            rows_a_after = list(ctx.get_table("a").scan_with_condition(lambda _: True))
            assert len(rows_a_after) == 0

    def test_drop_table_persistence(self) -> None:
        """重启后验证 Catalog 是否确实删除了该表。"""
        import tempfile
        from pathlib import Path
        from bplus_tree.database_context import DatabaseContext
        from bplus_tree.sql_engine import execute_sql

        with tempfile.TemporaryDirectory() as d:
            path = Path(d)
            ctx = DatabaseContext(path)
            execute_sql("CREATE TABLE x (id INT, v VARCHAR(16))", db=ctx)
            execute_sql("CREATE TABLE y (id INT, v VARCHAR(16))", db=ctx)
            assert "x" in ctx._catalog.list_tables()
            assert "y" in ctx._catalog.list_tables()

            ctx.drop_table("x")
            assert "x" not in ctx._catalog.list_tables()
            assert "y" in ctx._catalog.list_tables()

            ctx2 = DatabaseContext(path)
            ctx2.load_tables()
            assert "x" not in ctx2._catalog.list_tables()
            assert "y" in ctx2._catalog.list_tables()
            assert "y" in ctx2._tables

    def test_parse_drop_table(self) -> None:
        p = parse_sql("DROP TABLE users")
        assert p.table == "users"

    def test_execute_drop_table(self) -> None:
        import tempfile
        from pathlib import Path
        from bplus_tree.database_context import DatabaseContext
        from bplus_tree.sql_engine import execute_sql

        with tempfile.TemporaryDirectory() as d:
            ctx = DatabaseContext(Path(d))
            execute_sql("CREATE TABLE z (id INT PRIMARY KEY)", db=ctx)
            assert "z" in ctx._catalog.list_tables()

            msg, _, _ = execute_sql("DROP TABLE z", db=ctx)
            assert "DROP TABLE ok" in msg
            assert "z" not in ctx._catalog.list_tables()


class TestWireProtocol:
    """Wire Protocol 编码测试。"""

    def test_encode_response(self) -> None:
        payload = _encode_response_correct("OK", "1 row", [[1, "a"]], ["id", "name"])
        text = payload.decode("utf-8")
        assert "OK" in text
        assert "id" in text
        assert "name" in text
        assert "1" in text
