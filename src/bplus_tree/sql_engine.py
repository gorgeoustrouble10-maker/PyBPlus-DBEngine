"""
极简 SQL 解析器与执行引擎。

English: Minimal SQL parser and execution engine; maps to RowTable operations.
Chinese: 极简 SQL 解析器与执行引擎；映射到 RowTable 操作。
Japanese: ミニマル SQL パーサーと実行エンジン；RowTable 操作にマッピング。
"""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from bplus_tree.errors import (
    EmptySQLError,
    SQLSyntaxError,
    UnknownTableError,
    UnsupportedSQLError,
)
from bplus_tree.schema import Schema
from bplus_tree.table import RowTable, Tuple


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
    English: Parsed SELECT statement.
    Chinese: 解析后的 SELECT 语句。
    Japanese: パース済み SELECT 文。
    """

    columns: list[str]  # ["*"] or ["id", "name", ...]
    table: str
    where: Optional[str] = None  # "id >= 1 AND id <= 10" or None
    start_key: Optional[Any] = None
    end_key: Optional[Any] = None


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


def parse_sql(sql: str) -> ParsedSelect | ParsedInsert | ParsedDelete | ParsedCreateTable:
    """
    English: Parse SQL string into SELECT/INSERT/DELETE/CREATE TABLE structure.
    Chinese: 将 SQL 字符串解析为 SELECT/INSERT/DELETE/CREATE TABLE 结构。
    Japanese: SQL 文字列を SELECT/INSERT/DELETE/CREATE TABLE 構造にパースします。

    Supported forms:
    - CREATE TABLE name (col1 INT, col2 VARCHAR(32), ...)
    - SELECT * FROM t [WHERE id >= 1 AND id <= 10]
    - INSERT INTO t (col1, col2) VALUES (v1, v2)
    - DELETE FROM t WHERE id = 5
    """
    sql = sql.strip().rstrip(";").strip()
    if not sql:
        raise EmptySQLError()

    upper = sql.upper()
    if upper.startswith("CREATE TABLE"):
        return _parse_create_table(sql)
    if upper.startswith("SELECT"):
        return _parse_select(sql)
    if upper.startswith("INSERT"):
        return _parse_insert(sql)
    if upper.startswith("DELETE"):
        return _parse_delete(sql)

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
        if "PRIMARY" in part.upper() and "KEY" in part.upper():
            primary_key = col_name
        columns.append((col_name, col_type))
    if not primary_key and columns:
        primary_key = columns[0][0]
    if not primary_key:
        raise SQLSyntaxError("CREATE TABLE requires a primary key")
    return ParsedCreateTable(table=table_name, columns=columns, primary_key=primary_key)


def _parse_select(sql: str) -> ParsedSelect:
    """Parse SELECT * FROM table [WHERE ...]."""
    m = re.match(
        r"SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    if not m:
        raise ValueError(f"Invalid SELECT: {sql[:80]}")
    cols_str, table, where = m.group(1).strip(), m.group(2).strip(), m.group(3)
    columns = [c.strip() for c in cols_str.split(",")]
    start_key, end_key = _parse_where_range(where) if where else (None, None)
    return ParsedSelect(columns=columns, table=table, where=where, start_key=start_key, end_key=end_key)


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


def execute_sql(
    sql: str,
    table: Optional[RowTable] = None,
    db: Optional[Any] = None,
    tx: Optional[Any] = None,
) -> tuple[str, list[list[Any]], Optional[list[str]]]:
    """
    English: Execute SQL; use db for multi-table/CREATE, else single table.
    Chinese: 执行 SQL；db 用于多表/CREATE，否则使用单表。
    Japanese: SQL を実行；db でマルチテーブル/CREATE、否则は単一テーブル。

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

    if isinstance(parsed, ParsedSelect):
        tbl = _get_table(parsed.table)
        columns = parsed.columns
        schema = tbl._schema
        names = schema.field_names()
        if columns != ["*"]:
            for c in columns:
                if c not in names:
                    raise ValueError(f"Unknown column: {c}")
            names = columns
        rows: list[list[Any]] = []
        for r in tbl.scan_with_condition(
            lambda _: True,
            start_key=parsed.start_key,
            end_key=parsed.end_key,
            read_view=None,
        ):
            rows.append([r.get_field(n) for n in names])
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
        tbl.insert_row(values, transaction=tx)
        return ("INSERT ok", [], None)

    if isinstance(parsed, ParsedDelete):
        tbl = _get_table(parsed.table)
        if parsed.pk_value is None:
            raise SQLSyntaxError("DELETE requires WHERE id = value")
        tbl.delete_row(parsed.pk_value, transaction=tx)
        return ("DELETE ok", [], None)

    raise ValueError("Unsupported statement")
