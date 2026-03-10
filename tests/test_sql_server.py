"""
Phase 15 测试：SQL 解析器、执行引擎、Wire Protocol。
"""

import sys
from pathlib import Path

import pytest

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


class TestPhase18:
    """Phase 18: ORDER BY, LIMIT, OFFSET, COUNT(*), SHOW TABLES, SHOW STATS, SQL security."""

    def test_order_by_limit_offset(self) -> None:
        schema = Schema(fields=[("id", "INT"), ("name", "VARCHAR(32)")])
        table = RowTable(schema, primary_key="id")
        for i in [3, 1, 4, 2]:
            table.insert_row([i, f"x{i}"])
        msg, rows, cols = execute_sql(
            "SELECT id, name FROM t ORDER BY id ASC LIMIT 2 OFFSET 1",
            table=table,
        )
        assert cols == ["id", "name"]
        assert rows == [[2, "x2"], [3, "x3"]]

        msg2, rows2, _ = execute_sql(
            "SELECT * FROM t ORDER BY id DESC LIMIT 1",
            table=table,
        )
        assert rows2 == [[4, "x4"]]

    def test_count_star(self) -> None:
        schema = Schema(fields=[("id", "INT"), ("x", "VARCHAR(8)")])
        table = RowTable(schema, primary_key="id")
        table.insert_row([1, "a"])
        table.insert_row([2, "b"])
        msg, rows, cols = execute_sql("SELECT COUNT(*) FROM t", table=table)
        assert cols == ["COUNT(*)"]
        assert rows == [[2]]

    def test_show_tables(self) -> None:
        import tempfile
        from pathlib import Path
        from bplus_tree.database_context import DatabaseContext

        with tempfile.TemporaryDirectory() as d:
            ctx = DatabaseContext(Path(d))
            execute_sql("CREATE TABLE a (id INT)", db=ctx)
            execute_sql("CREATE TABLE b (id INT)", db=ctx)
            msg, rows, cols = execute_sql("SHOW TABLES", db=ctx)
            assert cols == ["Tables"]
            assert len(rows) == 2
            names = {r[0] for r in rows}
            assert names == {"a", "b"}

    def test_show_stats(self) -> None:
        import tempfile
        from pathlib import Path
        from bplus_tree.database_context import DatabaseContext
        from bplus_tree.transaction import TransactionManager

        with tempfile.TemporaryDirectory() as d:
            ctx = DatabaseContext(Path(d))
            execute_sql("CREATE TABLE x (id INT)", db=ctx)
            tx_mgr = TransactionManager()
            tx = tx_mgr.begin()
            msg, rows, cols = execute_sql(
                "SHOW STATS", db=ctx, tx_manager=tx_mgr
            )
            assert "buffer_pool_hit_rate" in [r[0] for r in rows]
            assert "active_transactions" in [r[0] for r in rows]
            active_row = next(r for r in rows if r[0] == "active_transactions")
            assert active_row[1] == 1

    def test_sql_syntax_error_no_crash(self) -> None:
        from bplus_tree.sql_engine import parse_sql
        from bplus_tree.errors import SQLSyntaxError

        with pytest.raises(SQLSyntaxError) as ei:
            parse_sql("SELECT ??? FROM")
        assert ei.value.code == 1064

    def test_varchar_limit(self) -> None:
        from bplus_tree.errors import DataLimitError

        with pytest.raises(DataLimitError):
            parse_sql("CREATE TABLE t (id INT, x VARCHAR(99999))")


class TestPhase19Savepoints:
    """Phase 19: 事务保存点 SAVEPOINT / ROLLBACK TO。"""

    def test_savepoint_rollback_to(self) -> None:
        """SAVEPOINT 创建后，ROLLBACK TO 仅回滚之后的操作。"""
        from bplus_tree.schema import Schema
        from bplus_tree.table import RowTable
        from bplus_tree.transaction import Transaction, TransactionManager

        schema = Schema(fields=[("id", "INT"), ("v", "VARCHAR(8)")])
        table = RowTable(schema, primary_key="id")
        tx_mgr = TransactionManager()
        tx = tx_mgr.begin()

        table.insert_row([1, "a"], transaction=tx)
        tx.savepoint("sp1")
        table.insert_row([2, "b"], transaction=tx)
        table.insert_row([3, "c"], transaction=tx)
        tx.rollback_to("sp1")

        rows = list(table.scan_with_condition(lambda _: True))
        assert len(rows) == 1
        assert rows[0].get_field("id") == 1

        tx.rollback()
        tx_mgr.abort(tx)

    def test_parse_savepoint_rollback_to(self) -> None:
        from bplus_tree.sql_engine import parse_sql

        sp = parse_sql("SAVEPOINT my_sp")
        assert hasattr(sp, "name") and sp.name == "my_sp"

        rb = parse_sql("ROLLBACK TO my_sp")
        assert hasattr(rb, "name") and rb.name == "my_sp"


class TestBufferPoolPersistence:
    """Phase 21: 统一持久化 - BufferPool 持久化验证。"""

    def test_buffer_pool_persistence(self) -> None:
        """create_table -> insert -> checkpoint -> 新上下文 load -> 数据可查。"""
        import tempfile
        from pathlib import Path
        from bplus_tree.database_context import DatabaseContext
        from bplus_tree.sql_engine import execute_sql

        with tempfile.TemporaryDirectory() as d:
            path = Path(d)
            ctx = DatabaseContext(path)
            execute_sql("CREATE TABLE bp (id INT, v VARCHAR(16))", db=ctx)
            execute_sql("INSERT INTO bp (id, v) VALUES (1, 'a')", db=ctx)
            execute_sql("INSERT INTO bp (id, v) VALUES (2, 'b')", db=ctx)
            ctx.checkpoint_all()

            ctx2 = DatabaseContext(path)
            ctx2.load_tables()
            msg, rows, cols = execute_sql("SELECT * FROM bp", db=ctx2)
            assert len(rows) == 2
            ids = sorted(r[0] for r in rows)
            assert ids == [1, 2]


class TestWireProtocol:
    """Wire Protocol 编码测试。"""

    def test_encode_response(self) -> None:
        payload = _encode_response_correct("OK", "1 row", [[1, "a"]], ["id", "name"])
        text = payload.decode("utf-8")
        assert "OK" in text
        assert "id" in text
        assert "name" in text
        assert "1" in text
