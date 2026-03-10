"""
Phase 15 测试：SQL 解析器、执行引擎、Wire Protocol。
"""

import sys
from pathlib import Path
from typing import Any

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

    def test_explain_select(self) -> None:
        """Phase 24: EXPLAIN SELECT 输出执行策略。"""
        schema = Schema(fields=[("id", "INT"), ("name", "VARCHAR(32)")])
        table = RowTable(schema, primary_key="id")
        execute_sql("INSERT INTO t (id, name) VALUES (1, 'a')", table)
        execute_sql("INSERT INTO t (id, name) VALUES (2, 'b')", table)

        msg, rows, cols = execute_sql("EXPLAIN SELECT * FROM t WHERE id >= 1 AND id <= 10", table)
        assert "(explain)" in msg
        assert cols == ["item", "value"]
        by_item = dict((r[0], r[1]) for r in rows)
        assert by_item["Query Type"] == "SELECT"
        assert by_item["Execution Strategy"] in ("TABLE_SCAN", "INDEX_SCAN")
        assert "Filter Predicates" in by_item

    def test_explain_insert(self) -> None:
        """Phase 24: EXPLAIN INSERT 输出。"""
        schema = Schema(fields=[("id", "INT"), ("name", "VARCHAR(32)")])
        table = RowTable(schema, primary_key="id")
        msg, rows, _ = execute_sql("EXPLAIN INSERT INTO t (id, name) VALUES (1, 'x')", table)
        assert "(explain)" in msg
        by_item = dict((r[0], r[1]) for r in rows)
        assert by_item["Query Type"] == "INSERT"
        assert by_item["Target Table"] == "t"

    def test_where_in(self) -> None:
        """Phase 25: WHERE id IN (1, 2, 3) 多值匹配。"""
        schema = Schema(fields=[("id", "INT"), ("name", "VARCHAR(32)")])
        table = RowTable(schema, primary_key="id")
        for i in range(1, 6):
            execute_sql(f"INSERT INTO t (id, name) VALUES ({i}, 'v{i}')", table)
        msg, rows, _ = execute_sql("SELECT * FROM t WHERE id IN (1, 3, 5)", table)
        assert len(rows) == 3
        ids = sorted(r[0] for r in rows)
        assert ids == [1, 3, 5]


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


class TestFailoverPromotion:
    """Phase 25: Slave 无心跳 5 秒后 promote_to_master。"""

    def test_failover_promotion(self) -> None:
        """Slave 连接后若 Master 无 WAL 推送超过 failover 超时，触发 promote。"""
        import socket
        import struct
        import threading
        import time

        from bplus_tree.schema import Schema
        from bplus_tree.table import RowTable
        from bplus_tree.replication import ReplicationSubscriber

        schema = Schema(fields=[("id", "INT"), ("v", "VARCHAR(8)")])
        table = RowTable(schema, primary_key="id")
        tables = {"t": table}
        replication_info: dict[str, Any] = {"node_role": "SLAVE"}

        # Mock master: send one WAL line then close (simulates Master dying)
        sent = threading.Event()

        def mock_master() -> None:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
            sock.listen(1)
            sent.port = port
            conn, _ = sock.accept()
            msg = b"t\tTX_BEGIN 1\n"
            conn.sendall(struct.pack("<I", len(msg)) + msg)
            conn.close()
            sock.close()
            sent.set()

        t = threading.Thread(target=mock_master)
        t.start()
        time.sleep(0.1)
        port = sent.port

        subscriber = ReplicationSubscriber(
            "127.0.0.1",
            port,
            tables,
            replication_info_ref=replication_info,
            failover_timeout_sec=0.3,
        )
        subscriber.start()
        t.join(timeout=2.0)
        time.sleep(0.5)
        assert replication_info.get("node_role") == "MASTER"


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


class TestSemiSyncConsistency:
    """Phase 26a: 半同步复制 - Slave ACK、Master 等待、超时降级。"""

    def test_semi_sync_consistency(self) -> None:
        """Semi-sync 模式：Master 等待 Slave ACK 后才返回；Slave 无连接时超时降级。"""
        import shutil
        import tempfile
        import time
        from pathlib import Path

        from bplus_tree.database_context import DatabaseContext
        from bplus_tree.replication import ReplicationPublisher, ReplicationSubscriber

        d = tempfile.mkdtemp()
        publisher = None
        subscriber = None
        try:
            path = Path(d)
            ctx = DatabaseContext(path)
            ctx.create_table("t", Schema(fields=[("id", "INT"), ("v", "VARCHAR(8)")]), "id")

            repl_port = 18770
            replication_info: dict[str, Any] = {
                "node_role": "MASTER",
                "replication_type": "SEMI_SYNC",
            }
            publisher = ReplicationPublisher(ctx._data_dir, ctx.get_tables(), repl_port)
            publisher.start()

            from bplus_tree.sql_engine import execute_sql

            # 无 Slave：INSERT 应超时并降级为 ASYNC
            execute_sql(
                "INSERT INTO t (id, v) VALUES (1, 'a')",
                db=ctx,
                replication_info=replication_info,
                replication_publisher=publisher,
                replication_timeout=0.05,
            )
            assert replication_info.get("replication_type") == "ASYNC"

            # 重置为 SEMI_SYNC 并启动 Slave
            replication_info["replication_type"] = "SEMI_SYNC"
            tables = ctx.get_tables()
            subscriber = ReplicationSubscriber(
                "127.0.0.1",
                repl_port,
                tables,
                replication_info_ref={"node_role": "SLAVE"},
                failover_timeout_sec=10.0,
            )
            subscriber.start()
            time.sleep(0.3)

            # 有 Slave：INSERT 应成功，Slave 收到并 ACK
            execute_sql(
                "INSERT INTO t (id, v) VALUES (2, 'b')",
                db=ctx,
                replication_info=replication_info,
                replication_publisher=publisher,
                replication_timeout=0.2,
            )
            assert replication_info.get("replication_type") == "SEMI_SYNC"

            time.sleep(0.2)
            slave_t = tables["t"]
            rows = list(slave_t.scan_with_condition(lambda _: True))
            ids = sorted(r.get_field("id") for r in rows)
            assert 2 in ids
        finally:
            if subscriber:
                subscriber.stop()
            if publisher:
                publisher.stop()
            time.sleep(0.2)
            shutil.rmtree(d, ignore_errors=True)


class TestPhase26bJoin:
    """Phase 26b: Nested Loop Join、EXPLAIN JOIN、AST 解析。"""

    def test_join_nested_loop(self) -> None:
        """SELECT t1.a, t2.b FROM t1 JOIN t2 ON t1.id = t2.id"""
        import tempfile
        from pathlib import Path

        from bplus_tree.database_context import DatabaseContext
        from bplus_tree.sql_engine import execute_sql, parse_sql

        with tempfile.TemporaryDirectory() as d:
            ctx = DatabaseContext(Path(d))
            execute_sql("CREATE TABLE t1 (id INT, a VARCHAR(8))", db=ctx)
            execute_sql("CREATE TABLE t2 (id INT, b VARCHAR(8))", db=ctx)
            execute_sql("INSERT INTO t1 (id, a) VALUES (1, 'x')", db=ctx)
            execute_sql("INSERT INTO t1 (id, a) VALUES (2, 'y')", db=ctx)
            execute_sql("INSERT INTO t2 (id, b) VALUES (1, 'p')", db=ctx)
            execute_sql("INSERT INTO t2 (id, b) VALUES (2, 'q')", db=ctx)

            msg, rows, cols = execute_sql(
                "SELECT t1.a, t2.b FROM t1 JOIN t2 ON t1.id = t2.id",
                db=ctx,
            )
            assert len(rows) == 2
            by_a = {r[0]: r[1] for r in rows}
            assert by_a.get("x") == "p"
            assert by_a.get("y") == "q"

    def test_explain_join(self) -> None:
        """EXPLAIN SELECT ... JOIN 展示执行顺序与代价"""
        import tempfile
        from pathlib import Path

        from bplus_tree.database_context import DatabaseContext
        from bplus_tree.sql_engine import execute_sql

        with tempfile.TemporaryDirectory() as d:
            ctx = DatabaseContext(Path(d))
            execute_sql("CREATE TABLE t1 (id INT, a INT)", db=ctx)
            execute_sql("CREATE TABLE t2 (id INT, b INT)", db=ctx)
            execute_sql("INSERT INTO t1 (id, a) VALUES (1, 10)", db=ctx)
            execute_sql("INSERT INTO t2 (id, b) VALUES (1, 20)", db=ctx)

            msg, rows, cols = execute_sql(
                "EXPLAIN SELECT t1.a, t2.b FROM t1 JOIN t2 ON t1.id = t2.id",
                db=ctx,
            )
            by_item = {r[0]: r[1] for r in rows}
            assert "SELECT (JOIN)" in str(by_item.get("Query Type", ""))
            assert "NESTED_LOOP_JOIN" in str(by_item.get("Join Type", ""))
            assert "Join Condition" in by_item


class TestSetGlobal:
    """Phase 26a: SET GLOBAL replication_type。"""

    def test_set_global_replication_type(self) -> None:
        from bplus_tree.sql_engine import parse_sql, execute_sql

        p = parse_sql("SET GLOBAL replication_type = 'SEMI_SYNC'")
        assert p.var == "replication_type"
        assert p.value == "SEMI_SYNC"

        repl_info: dict[str, Any] = {"replication_type": "ASYNC"}
        msg, _, _ = execute_sql(
            "SET GLOBAL replication_type = 'SEMI_SYNC'",
            replication_info=repl_info,
        )
        assert repl_info["replication_type"] == "SEMI_SYNC"

        msg, _, _ = execute_sql(
            "SET GLOBAL replication_type = 'ASYNC'",
            replication_info=repl_info,
        )
        assert repl_info["replication_type"] == "ASYNC"


class TestWireProtocol:
    """Wire Protocol 编码测试。"""

    def test_encode_response(self) -> None:
        payload = _encode_response_correct("OK", "1 row", [[1, "a"]], ["id", "name"])
        text = payload.decode("utf-8")
        assert "OK" in text
        assert "id" in text
        assert "name" in text
        assert "1" in text
