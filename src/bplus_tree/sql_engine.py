"""
极简 SQL 解析器与执行引擎；支持 ORDER BY、LIMIT、COUNT(*)、SHOW TABLES、SHOW STATS。

English: Minimal SQL parser and execution engine; ORDER BY, LIMIT, COUNT(*), SHOW TABLES, SHOW STATS.
Chinese: 极简 SQL 解析器与执行引擎；支持 ORDER BY、LIMIT、COUNT(*)、SHOW TABLES、SHOW STATS。
Japanese: ミニマル SQL パーサーと実行エンジン；ORDER BY、LIMIT、COUNT(*)、SHOW TABLES、SHOW STATS 対応。
"""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from bplus_tree.errors import (
    DBError,
    DataLimitError,
    EmptySQLError,
    SQLSyntaxError,
    UnknownColumnError,
    UnknownTableError,
    UnsupportedSQLError,
)
from bplus_tree.schema import Schema
from bplus_tree.table import RowTable, Tuple

# Security limits: VARCHAR max length, single INSERT payload size (bytes)
MAX_VARCHAR_LENGTH: int = 4096
MAX_INSERT_BYTES: int = 1024 * 1024


@dataclass
class ParsedDropTable:
    """
    English: Parsed DROP TABLE statement.
    Chinese: 解析后的 DROP TABLE 语句。
    Japanese: パース済み DROP TABLE 文。
    """
    table: str


@dataclass
class ParsedCreateTable:
    """
    English: Parsed CREATE TABLE statement.
    Chinese: 解析后的 CREATE TABLE 语句。
    Japanese: パース済み CREATE TABLE 文。
    """

    table: str
    columns: list[tuple[str, str]]  # (name, type)
    primary_key: str


@dataclass
class ParsedSelect:
    """
    English: Parsed SELECT statement with ORDER BY, LIMIT, OFFSET, COUNT(*).
    Chinese: 解析后的 SELECT 语句；支持 ORDER BY、LIMIT、OFFSET、COUNT(*)。
    Japanese: パース済み SELECT 文；ORDER BY、LIMIT、OFFSET、COUNT(*) 対応。
    """

    columns: list[str]  # ["*"] or ["id", "name", ...] or ["COUNT(*)"] for aggregation
    table: str
    where: Optional[str] = None
    start_key: Optional[Any] = None
    end_key: Optional[Any] = None
    order_by_col: Optional[str] = None
    order_desc: bool = False  # True = DESC, False = ASC
    limit: Optional[int] = None
    offset: Optional[int] = None
    is_count: bool = False


@dataclass
class ParsedShowTables:
    """Parsed SHOW TABLES statement."""


@dataclass
class ParsedShowStats:
    """Parsed SHOW STATS statement."""


@dataclass
class ParsedInsert:
    """
    English: Parsed INSERT statement.
    Chinese: 解析后的 INSERT 语句。
    Japanese: パース済み INSERT 文。
    """

    table: str
    columns: list[str]
    values: list[Any]


@dataclass
class ParsedDelete:
    """
    English: Parsed DELETE statement.
    Chinese: 解析后的 DELETE 语句。
    Japanese: パース済み DELETE 文。
    """

    table: str
    where: Optional[str] = None
    pk_value: Optional[Any] = None  # 若 WHERE id = N 则提取出 N


@dataclass
class ParsedSavepoint:
    """
    English: Parsed SAVEPOINT name statement.
    Chinese: 解析后的 SAVEPOINT name 语句。
    Japanese: パース済み SAVEPOINT name 文。
    """
    name: str


@dataclass
class ParsedRollbackTo:
    """
    English: Parsed ROLLBACK TO name statement.
    Chinese: 解析后的 ROLLBACK TO name 语句。
    Japanese: パース済み ROLLBACK TO name 文。
    """
    name: str


def parse_sql(
    sql: str,
) -> ParsedSelect | ParsedInsert | ParsedDelete | ParsedCreateTable | ParsedDropTable | ParsedShowTables | ParsedShowStats:
    """
    English: Parse SQL string; malformed SQL returns 1064 Syntax Error, never crashes.
    Chinese: 解析 SQL；畸形 SQL 返回 1064 语法错误，不会导致进程崩溃。
    Japanese: SQL をパース；不正 SQL は 1064 構文エラーを返し、クラッシュしない。
    """
    try:
        return _parse_sql_impl(sql)
    except DBError:
        raise
    except Exception:
        raise SQLSyntaxError("SQL syntax error")


def _parse_sql_impl(sql: str):
    """Internal parse; exceptions converted by parse_sql."""
    sql = sql.strip().rstrip(";").strip()
    if not sql:
        raise EmptySQLError()

    upper = sql.upper()
    if upper.startswith("CREATE TABLE"):
        return _parse_create_table(sql)
    if upper.startswith("DROP TABLE"):
        return _parse_drop_table(sql)
    if upper.startswith("SHOW TABLES"):
        return ParsedShowTables()
    if upper.startswith("SHOW STATS"):
        return ParsedShowStats()
    if upper.startswith("SELECT"):
        return _parse_select(sql)
    if upper.startswith("INSERT"):
        return _parse_insert(sql)
    if upper.startswith("DELETE"):
        return _parse_delete(sql)
    if upper.startswith("SAVEPOINT"):
        return _parse_savepoint(sql)
    if upper.startswith("ROLLBACK TO"):
        return _parse_rollback_to(sql)

    raise SQLSyntaxError(f"Unsupported SQL: {sql[:50]}...")


def _parse_create_table(sql: str) -> ParsedCreateTable:
    """
    English: Parse CREATE TABLE name (col1 INT, col2 VARCHAR(32), ...).
    Chinese: 解析 CREATE TABLE name (col1 INT, col2 VARCHAR(32), ...)。
    Japanese: CREATE TABLE name (col1 INT, col2 VARCHAR(32), ...) をパース。
    """
    m = re.match(
        r"CREATE\s+TABLE\s+(\w+)\s*\((.+)\)",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    if not m:
        raise SQLSyntaxError("Invalid CREATE TABLE syntax")
    table_name = m.group(1).strip()
    col_defs = m.group(2).strip()
    columns: list[tuple[str, str]] = []
    primary_key = ""
    for part in re.split(r",\s*(?![^(]*\))", col_defs):
        part = part.strip()
        if re.match(r"PRIMARY\s+KEY\s*\(\s*(\w+)\s*\)", part, re.IGNORECASE):
            pk_m = re.search(r"\(\s*(\w+)\s*\)", part, re.IGNORECASE)
            if pk_m:
                primary_key = pk_m.group(1).strip()
            continue
        col_m = re.match(r"(\w+)\s+(\w+(?:\(\d+\))?)\s*(?:PRIMARY\s+KEY)?", part, re.IGNORECASE)
        if not col_m:
            raise SQLSyntaxError(f"Invalid column definition: {part}")
        col_name, col_type = col_m.group(1).strip(), col_m.group(2).strip().upper()
        if col_type.startswith("VARCHAR("):
            vm = re.search(r"VARCHAR\s*\(\s*(\d+)\s*\)", col_type, re.IGNORECASE)
            if vm:
                vlen = int(vm.group(1))
                if vlen > MAX_VARCHAR_LENGTH:
                    raise DataLimitError(
                        f"VARCHAR length exceeds maximum {MAX_VARCHAR_LENGTH}"
                    )
        if "PRIMARY" in part.upper() and "KEY" in part.upper():
            primary_key = col_name
        columns.append((col_name, col_type))
    if not primary_key and columns:
        primary_key = columns[0][0]
    if not primary_key:
        raise SQLSyntaxError("CREATE TABLE requires a primary key")
    return ParsedCreateTable(table=table_name, columns=columns, primary_key=primary_key)


def _parse_drop_table(sql: str) -> ParsedDropTable:
    """
    English: Parse DROP TABLE name.
    Chinese: 解析 DROP TABLE name。
    Japanese: DROP TABLE name をパース。
    """
    m = re.match(r"DROP\s+TABLE\s+(\w+)", sql, re.IGNORECASE)
    if not m:
        raise SQLSyntaxError("Invalid DROP TABLE syntax")
    return ParsedDropTable(table=m.group(1).strip())


def _parse_select(sql: str) -> ParsedSelect:
    """
    English: Parse SELECT [COUNT(*)|*|cols] FROM t [WHERE ...] [ORDER BY col [ASC|DESC]] [LIMIT n] [OFFSET n].
    Chinese: 解析 SELECT，支持 ORDER BY、LIMIT、OFFSET、COUNT(*)。
    Japanese: SELECT をパース；ORDER BY、LIMIT、OFFSET、COUNT(*) 対応。
    """
    rest = sql.strip()
    order_by_col: Optional[str] = None
    order_desc = False
    limit: Optional[int] = None
    offset: Optional[int] = None

    def strip_trailing(pat: str):
        nonlocal rest
        m = re.search(pat, rest, re.IGNORECASE)
        if m:
            rest = rest[: m.start()].rstrip()
            return m
        return None

    if mo := strip_trailing(r"\s+OFFSET\s+(\d+)\s*$"):
        offset = int(mo.group(1))
    if mo := strip_trailing(r"\s+LIMIT\s+(\d+)\s*$"):
        limit = int(mo.group(1))
    if mo := strip_trailing(r"\s+ORDER\s+BY\s+(\w+)(?:\s+(ASC|DESC))?\s*$"):
        order_by_col = mo.group(1).strip()
        order_desc = (mo.group(2) or "").upper() == "DESC"

    m = re.match(
        r"SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?",
        rest,
        re.IGNORECASE | re.DOTALL,
    )
    if not m:
        raise SQLSyntaxError("Invalid SELECT syntax")
    cols_str, table, where = m.group(1).strip(), m.group(2).strip(), (m.group(3) or "").strip() or None
    columns = [c.strip() for c in cols_str.split(",")]
    is_count = len(columns) == 1 and columns[0].upper().replace(" ", "") == "COUNT(*)"
    if is_count:
        columns = ["COUNT(*)"]
    start_key, end_key = _parse_where_range(where) if where else (None, None)
    return ParsedSelect(
        columns=columns,
        table=table,
        where=where,
        start_key=start_key,
        end_key=end_key,
        order_by_col=order_by_col,
        order_desc=order_desc,
        limit=limit,
        offset=offset,
        is_count=is_count,
    )


def _parse_where_range(where: str) -> tuple[Optional[Any], Optional[Any]]:
    """
    English: Extract start_key, end_key from WHERE id >= X AND id <= Y.
    Chinese: 从 WHERE id >= X AND id <= Y 提取 start_key, end_key。
    Japanese: WHERE id >= X AND id <= Y から start_key, end_key を抽出。
    """
    start_key: Optional[Any] = None
    end_key: Optional[Any] = None
    where = where.strip()
    m_ge = re.search(r"id\s*>=\s*(-?\d+\.?\d*)", where, re.IGNORECASE)
    m_le = re.search(r"id\s*<=\s*(-?\d+\.?\d*)", where, re.IGNORECASE)
    m_gt = re.search(r"id\s*>\s*(-?\d+\.?\d*)", where, re.IGNORECASE)
    m_lt = re.search(r"id\s*<\s*(-?\d+\.?\d*)", where, re.IGNORECASE)
    m_eq = re.search(r"id\s*=\s*(-?\d+\.?\d*|'[^']*'|\"[^\"]*\")", where, re.IGNORECASE)

    if m_eq:
        v = m_eq.group(1)
        start_key = end_key = _parse_value(v)
        return start_key, end_key
    if m_ge:
        start_key = _parse_value(m_ge.group(1))
    if m_gt:
        v = _parse_value(m_gt.group(1))
        start_key = v + 1 if isinstance(v, (int, float)) else v
    if m_le:
        end_key = _parse_value(m_le.group(1))
    if m_lt:
        v = _parse_value(m_lt.group(1))
        end_key = v - 1 if isinstance(v, (int, float)) else v
    return start_key, end_key


def _parse_value(s: str) -> Any:
    """Parse quoted string or number."""
    s = s.strip()
    if s.startswith("'") and s.endswith("'"):
        return s[1:-1]
    if s.startswith('"') and s.endswith('"'):
        return s[1:-1]
    if "." in s:
        try:
            return float(s)
        except ValueError:
            return s
    try:
        return int(s)
    except ValueError:
        return s


def _parse_insert(sql: str) -> ParsedInsert:
    """Parse INSERT INTO t (c1,c2) VALUES (v1,v2) or INSERT INTO t VALUES (v1,v2)."""
    m = re.match(
        r"INSERT\s+INTO\s+(\w+)\s*\((.+?)\)\s*VALUES\s*\((.+)\)",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    if m:
        table, cols_str, vals_str = m.group(1), m.group(2), m.group(3)
        columns = [c.strip() for c in cols_str.split(",")]
        values = _parse_value_list(vals_str)
        if len(values) != len(columns):
            raise ValueError(f"Column count {len(columns)} != value count {len(values)}")
        return ParsedInsert(table=table, columns=columns, values=values)

    m2 = re.match(
        r"INSERT\s+INTO\s+(\w+)\s+VALUES\s*\((.+)\)",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    if m2:
        table, vals_str = m2.group(1), m2.group(2)
        values = _parse_value_list(vals_str)
        return ParsedInsert(table=table, columns=[], values=values)

    raise ValueError(f"Invalid INSERT: {sql[:80]}")


def _parse_value_list(s: str) -> list[Any]:
    """Parse (v1, v2, 'str', 3.14) -> list."""
    s = s.strip()
    result: list[Any] = []
    i = 0
    while i < len(s):
        while i < len(s) and s[i] in " \t,":
            i += 1
        if i >= len(s):
            break
        if s[i] in "'\"":
            q = s[i]
            i += 1
            start = i
            while i < len(s) and s[i] != q:
                if s[i] == "\\":
                    i += 1
                i += 1
            result.append(s[start:i].replace("\\" + q, q))
            i += 1
        else:
            start = i
            while i < len(s) and s[i] not in ",)":
                i += 1
            tok = s[start:i].strip()
            result.append(_parse_value(tok))
    return result


def _parse_savepoint(sql: str) -> ParsedSavepoint:
    """
    English: Parse SAVEPOINT name.
    Chinese: 解析 SAVEPOINT name。
    Japanese: SAVEPOINT name をパース。
    """
    m = re.match(r"SAVEPOINT\s+(\w+)", sql, re.IGNORECASE)
    if not m:
        raise SQLSyntaxError("Invalid SAVEPOINT syntax")
    return ParsedSavepoint(name=m.group(1).strip())


def _parse_rollback_to(sql: str) -> ParsedRollbackTo:
    """
    English: Parse ROLLBACK TO name.
    Chinese: 解析 ROLLBACK TO name。
    Japanese: ROLLBACK TO name をパース。
    """
    m = re.match(r"ROLLBACK\s+TO\s+(\w+)", sql, re.IGNORECASE)
    if not m:
        raise SQLSyntaxError("Invalid ROLLBACK TO syntax")
    return ParsedRollbackTo(name=m.group(1).strip())


def _parse_delete(sql: str) -> ParsedDelete:
    """Parse DELETE FROM t WHERE id = 5."""
    m = re.match(
        r"DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    if not m:
        raise ValueError(f"Invalid DELETE: {sql[:80]}")
    table, where = m.group(1).strip(), (m.group(2).strip() if m.group(2) else None)
    pk_value = None
    if where:
        m_eq = re.search(r"id\s*=\s*(-?\d+\.?\d*|'[^']*'|\"[^\"]*\")", where, re.IGNORECASE)
        if m_eq:
            pk_value = _parse_value(m_eq.group(1))
    return ParsedDelete(table=table, where=where, pk_value=pk_value)


def _execute_show_stats(
    db: Optional[Any], tx_manager: Optional[Any]
) -> tuple[str, list[list[Any]], Optional[list[str]]]:
    """
    English: Execute SHOW STATS; buffer hit rate, active tx count, table files and sizes.
    Chinese: 执行 SHOW STATS；Buffer 命中率、活跃事务数、表文件及大小。
    Japanese: SHOW STATS を実行；Buffer ヒット率、アクティブ tx 数、テーブルファイルとサイズ。
    """
    rows: list[list[Any]] = []
    bp_hit = "N/A"
    if db is not None:
        pool = getattr(db, "_buffer_pool", None)
        if pool is not None and hasattr(pool, "hit_rate"):
            bp_hit = f"{getattr(pool, 'hit_rate', 0):.2%}"
    rows.append(["buffer_pool_hit_rate", bp_hit])

    active_tx = 0
    if tx_manager is not None:
        active_tx = len(tx_manager.get_active_ids())
    rows.append(["active_transactions", active_tx])

    if db is not None:
        data_dir = getattr(db, "_data_dir", None)
        catalog = getattr(db, "_catalog", None)
        if catalog is not None:
            for tname in catalog.list_tables():
                size = 0
                if data_dir:
                    for suf in [f"wal_{tname}.log", f"{tname}.db", f"{tname}.idx"]:
                        p = Path(data_dir) / suf
                        if p.exists():
                            size += p.stat().st_size
                rows.append([f"table_{tname}_bytes", size])
    return ("(stats)", rows, ["metric", "value"])


def _estimate_insert_size(values: list[Any]) -> int:
    """Estimate total byte size of INSERT values for security limit."""
    total = 0
    for v in values:
        if isinstance(v, str):
            total += len(v.encode("utf-8"))
        elif isinstance(v, (int, float)):
            total += 8
        else:
            total += 64
    return total


def execute_sql(
    sql: str,
    table: Optional[RowTable] = None,
    db: Optional[Any] = None,
    tx: Optional[Any] = None,
    tx_manager: Optional[Any] = None,
) -> tuple[str, list[list[Any]], Optional[list[str]]]:
    """
    English: Execute SQL; use db for multi-table/CREATE, tx_manager for SHOW STATS.
    Chinese: 执行 SQL；db 用于多表/CREATE，tx_manager 用于 SHOW STATS。
    Japanese: SQL を実行；db でマルチテーブル/CREATE、tx_manager で SHOW STATS。

    Returns:
        (status_message, rows, columns).
    """
    parsed = parse_sql(sql)

    def _get_table(name: str) -> RowTable:
        if db is not None:
            return db.get_table(name)
        if table is None:
            raise UnknownTableError(name)
        return table

    if isinstance(parsed, ParsedCreateTable):
        if db is None:
            raise UnsupportedSQLError("CREATE TABLE requires DatabaseContext")
        schema = Schema(fields=parsed.columns)
        db.create_table(parsed.table, schema, parsed.primary_key)
        return ("CREATE TABLE ok", [], None)

    if isinstance(parsed, ParsedDropTable):
        if db is None:
            raise UnsupportedSQLError("DROP TABLE requires DatabaseContext")
        db.drop_table(parsed.table)
        return ("DROP TABLE ok", [], None)

    if isinstance(parsed, ParsedShowTables):
        if db is None:
            raise UnsupportedSQLError("SHOW TABLES requires DatabaseContext")
        names = db._catalog.list_tables()
        rows = [[n] for n in names]
        return (f"({len(rows)} tables)", rows, ["Tables"])

    if isinstance(parsed, ParsedShowStats):
        return _execute_show_stats(db, tx_manager)

    if isinstance(parsed, ParsedSelect):
        tbl = _get_table(parsed.table)
        columns = parsed.columns
        schema = tbl._schema
        names = schema.field_names()
        if columns != ["*"] and not parsed.is_count:
            for c in columns:
                if c not in names:
                    raise UnknownColumnError(c)
            names = columns
        rows: list[list[Any]] = []
        for r in tbl.scan_with_condition(
            lambda _: True,
            start_key=parsed.start_key,
            end_key=parsed.end_key,
            read_view=None,
        ):
            rows.append([r.get_field(n) for n in names])

        if parsed.is_count:
            rows = [[len(rows)]]
            names = ["COUNT(*)"]
            return ("(1 row)", rows, names)

        if parsed.order_by_col:
            if parsed.order_by_col not in names:
                raise UnknownColumnError(parsed.order_by_col)
            col_idx = names.index(parsed.order_by_col)
            rows.sort(key=lambda r: r[col_idx], reverse=parsed.order_desc)

        if parsed.offset is not None:
            rows = rows[parsed.offset:]
        if parsed.limit is not None:
            rows = rows[: parsed.limit]

        return (f"({len(rows)} rows)", rows, names)

    if isinstance(parsed, ParsedInsert):
        tbl = _get_table(parsed.table)
        values = parsed.values
        schema = tbl._schema
        if not parsed.columns:
            if len(values) != len(schema):
                raise ValueError(f"Expected {len(schema)} values, got {len(values)}")
        else:
            names = schema.field_names()
            types = [f[1] for f in schema._fields]
            ordered: list[Any] = []
            for j, (nm, typ) in enumerate(zip(names, types)):
                if nm in parsed.columns:
                    ordered.append(values[parsed.columns.index(nm)])
                elif typ == "INT":
                    ordered.append(0)
                elif typ == "FLOAT":
                    ordered.append(0.0)
                elif typ.startswith("VARCHAR"):
                    ordered.append("")
                else:
                    ordered.append(None)
            values = ordered
        size = _estimate_insert_size(values)
        if size > MAX_INSERT_BYTES:
            raise DataLimitError(
                f"INSERT data size ({size} bytes) exceeds limit ({MAX_INSERT_BYTES})"
            )
        tbl.insert_row(values, transaction=tx)
        return ("INSERT ok", [], None)

    if isinstance(parsed, ParsedDelete):
        tbl = _get_table(parsed.table)
        if parsed.pk_value is None:
            raise SQLSyntaxError("DELETE requires WHERE id = value")
        tbl.delete_row(parsed.pk_value, transaction=tx)
        return ("DELETE ok", [], None)

    raise ValueError("Unsupported statement")
