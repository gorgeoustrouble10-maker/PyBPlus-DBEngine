"""
Phase 26b: 基于 sqlglot AST 的 SQL 解析器。
将 SELECT、INSERT 从正则迁移到 AST 节点提取。
"""

from typing import Any, Optional

from bplus_tree.errors import EmptySQLError, SQLSyntaxError

try:
    import sqlglot
    from sqlglot import exp as sql_exp

    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False


def _parse_value_from_ast(node: Any) -> Any:
    """从 sqlglot 表达式节点提取 Python 值；保持 int/float 类型。"""
    if node is None:
        return None
    if isinstance(node, sql_exp.Literal):
        v = node.this
        is_str = node.args.get("is_string", True)
        if isinstance(v, (int, float)) and not is_str:
            return v
        s = str(v)
        if not is_str:
            try:
                return int(s)
            except ValueError:
                try:
                    return float(s)
                except ValueError:
                    pass
        return s
    s = node.sql() if hasattr(node, "sql") else str(node)
    s = s.strip()
    if s.startswith("'") and s.endswith("'"):
        return s[1:-1].replace("''", "'")
    if s.startswith('"') and s.endswith('"'):
        return s[1:-1].replace('""', '"')
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s)
    except ValueError:
        pass
    return s


def parse_select_ast(sql: str) -> Optional[dict[str, Any]]:
    """
    用 sqlglot 解析 SELECT，返回与 ParsedSelect 兼容的 dict。
    支持单表与 JOIN。
    """
    if not SQLGLOT_AVAILABLE:
        return None
    try:
        tree = sqlglot.parse_one(sql)
    except Exception:
        return None
    if not isinstance(tree, sql_exp.Select):
        return None

    # 提取列
    columns: list[str] = []
    for col in tree.expressions or []:
        if isinstance(col, sql_exp.Star):
            columns = ["*"]
            break
        if isinstance(col, sql_exp.Count):
            columns = ["COUNT(*)"]
            break
        name = col.sql() if hasattr(col, "sql") else str(col)
        columns.append(name.strip())
    if not columns:
        columns = ["*"]

    is_count = len(columns) == 1 and columns[0].upper().replace(" ", "") == "COUNT(*)"

    # 提取表与 JOIN
    from_node = tree.find(sql_exp.From)
    if not from_node:
        return None
    tables = list(from_node.find_all(sql_exp.Table))
    if not tables:
        return None

    main_table = tables[0]
    table = main_table.name if hasattr(main_table, "name") else str(main_table)
    if hasattr(main_table, "alias") and main_table.alias:
        left_alias = main_table.alias  # noqa: F841
    else:
        left_alias = table

    join_tables: list[tuple[str, str]] = []
    join_on: list[tuple[str, str]] = []

    for j in tree.find_all(sql_exp.Join):
        jtable = j.this
        if isinstance(jtable, sql_exp.Table):
            jname = jtable.name if hasattr(jtable, "name") else str(jtable)
            jalias = jtable.alias if hasattr(jtable, "alias") and jtable.alias else jname
            join_tables.append((str(jalias), str(jname)))
        on_expr = j.args.get("on")
        if on_expr and isinstance(on_expr, sql_exp.EQ):
            left_col = on_expr.this.sql() if hasattr(on_expr.this, "sql") else str(on_expr.this)
            right_col = (
                on_expr.expression.sql()
                if hasattr(on_expr.expression, "sql")
                else str(on_expr.expression)
            )
            join_on.append((left_col.strip(), right_col.strip()))

    # 提取 WHERE
    where_node = tree.find(sql_exp.Where)
    where_str: Optional[str] = None
    start_key: Optional[Any] = None
    end_key: Optional[Any] = None
    in_values: Optional[list[Any]] = None

    if where_node and where_node.this:
        where_str = where_node.this.sql()
        # 解析 id >= x AND id <= y
        start_key, end_key = _parse_where_range_from_ast(where_node.this)
        in_values = _parse_where_in_from_ast(where_node.this)

    # ORDER BY, LIMIT, OFFSET
    order_by_col: Optional[str] = None
    order_desc = False
    for order in tree.find_all(sql_exp.Order):
        for key in order.expressions or []:
            col_node = key.this if hasattr(key, "this") else key
            if hasattr(col_node, "this") and hasattr(col_node.this, "name"):
                order_by_col = col_node.this.name
            else:
                order_by_col = col_node.sql() if hasattr(col_node, "sql") else str(col_node)
            order_desc = key.args.get("desc") is True
            break
        break

    limit_val: Optional[int] = None
    limit_node = tree.find(sql_exp.Limit)
    if limit_node:
        src = limit_node.this or limit_node.expression
        if src is not None:
            try:
                limit_val = int(_parse_value_from_ast(src))
            except (ValueError, TypeError):
                pass

    offset_val: Optional[int] = None
    offset_node = tree.find(sql_exp.Offset)
    if offset_node:
        src = offset_node.this or offset_node.expression
        if src is not None:
            try:
                offset_val = int(_parse_value_from_ast(src))
            except (ValueError, TypeError):
                pass

    return {
        "columns": columns,
        "table": table,
        "where": where_str,
        "start_key": start_key,
        "end_key": end_key,
        "in_values": in_values,
        "order_by_col": order_by_col,
        "order_desc": order_desc,
        "limit": limit_val,
        "offset": offset_val,
        "is_count": is_count,
        "join_tables": join_tables,
        "join_on": join_on,
    }


def _parse_where_range_from_ast(expr: Any) -> tuple[Optional[Any], Optional[Any]]:
    """从 WHERE 表达式提取 start_key, end_key。"""
    start_key: Optional[Any] = None
    end_key: Optional[Any] = None
    if expr is None:
        return start_key, end_key

    # 遍历 EQ, GTE, LTE, GT, LT, And
    def visit(e: Any) -> None:
        nonlocal start_key, end_key
        if e is None:
            return
        if isinstance(e, sql_exp.EQ):
            left = e.this.sql() if hasattr(e.this, "sql") else str(e.this)
            if "id" in left.lower() or "pk" in left.lower():
                v = _parse_value_from_ast(e.expression)
                start_key = end_key = v
        elif isinstance(e, sql_exp.GTE):
            left = e.this.sql() if hasattr(e.this, "sql") else str(e.this)
            if "id" in left.lower():
                start_key = _parse_value_from_ast(e.expression)
        elif isinstance(e, sql_exp.LTE):
            left = e.this.sql() if hasattr(e.this, "sql") else str(e.this)
            if "id" in left.lower():
                end_key = _parse_value_from_ast(e.expression)
        elif isinstance(e, sql_exp.GT):
            left = e.this.sql() if hasattr(e.this, "sql") else str(e.this)
            if "id" in left.lower():
                v = _parse_value_from_ast(e.expression)
                start_key = v + 1 if isinstance(v, (int, float)) else v
        elif isinstance(e, sql_exp.LT):
            left = e.this.sql() if hasattr(e.this, "sql") else str(e.this)
            if "id" in left.lower():
                v = _parse_value_from_ast(e.expression)
                end_key = v - 1 if isinstance(v, (int, float)) else v
        elif isinstance(e, sql_exp.And):
            visit(e.this)
            visit(e.expression)

    visit(expr)
    return start_key, end_key


def _parse_where_in_from_ast(expr: Any) -> Optional[list[Any]]:
    """从 WHERE 提取 IN (v1, v2, ...)。"""
    if expr is None:
        return None
    if isinstance(expr, sql_exp.In):
        values: list[Any] = []
        for v in (expr.expressions or []) if hasattr(expr, "expressions") else []:
            values.append(_parse_value_from_ast(v))
        return values if values else None
    if isinstance(expr, sql_exp.And):
        r = _parse_where_in_from_ast(expr.this)
        if r is not None:
            return r
        return _parse_where_in_from_ast(expr.expression)
    return None


def parse_insert_ast(sql: str) -> Optional[dict[str, Any]]:
    """用 sqlglot 解析 INSERT，返回与 ParsedInsert 兼容的 dict。"""
    if not SQLGLOT_AVAILABLE:
        return None
    try:
        tree = sqlglot.parse_one(sql)
    except Exception:
        return None
    if not isinstance(tree, sql_exp.Insert):
        return None

    table_name = ""
    columns: list[str] = []
    table = tree.this
    if isinstance(table, sql_exp.Schema):
        tbl = table.this
        table_name = tbl.name if hasattr(tbl, "name") else str(tbl)
        for c in (table.expressions or []):
            columns.append(c.name if hasattr(c, "name") else str(c))
    elif hasattr(table, "name"):
        table_name = table.name
    else:
        table_name = str(table)

    values: list[Any] = []
    expr = tree.expression
    if expr:
        if isinstance(expr, sql_exp.Values):
            for row in (expr.expressions or []):
                if isinstance(row, sql_exp.Tuple):
                    for v in (row.expressions or []):
                        values.append(_parse_value_from_ast(v))
                else:
                    values.append(_parse_value_from_ast(row))
        elif isinstance(expr, sql_exp.Tuple):
            for v in (expr.expressions or []):
                values.append(_parse_value_from_ast(v))

    return {"table": table_name, "columns": columns, "values": values}


def parse_with_ast(
    sql: str,
    parsed_select_cls: type,
    parsed_insert_cls: type,
) -> Optional[Any]:
    """
    尝试用 sqlglot 解析 SELECT/INSERT。
    成功则返回 ParsedSelect 或 ParsedInsert 实例；失败或非 SELECT/INSERT 返回 None。
    """
    sql = sql.strip().rstrip(";").strip()
    if not sql:
        raise EmptySQLError()
    upper = sql.upper()
    if upper.startswith("SELECT"):
        r = parse_select_ast(sql)
        if r is not None:
            return parsed_select_cls(
                columns=r["columns"],
                table=r["table"],
                where=r.get("where"),
                start_key=r.get("start_key"),
                end_key=r.get("end_key"),
                in_values=r.get("in_values"),
                order_by_col=r.get("order_by_col"),
                order_desc=r.get("order_desc", False),
                limit=r.get("limit"),
                offset=r.get("offset"),
                is_count=r.get("is_count", False),
                join_tables=r.get("join_tables") or [],
                join_on=r.get("join_on") or [],
            )
    elif upper.startswith("INSERT"):
        r = parse_insert_ast(sql)
        if r is not None:
            return parsed_insert_cls(
                table=r["table"],
                columns=r.get("columns") or [],
                values=r.get("values") or [],
            )
    return None
