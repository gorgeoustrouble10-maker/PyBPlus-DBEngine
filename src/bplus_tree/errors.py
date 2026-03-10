"""
统一数据库错误码体系。

English: Unified DBError code system; prevents server crash on invalid input.
Chinese: 统一 DBError 错误码体系；确保非法输入不会导致服务器崩溃。
Japanese: 統一 DBError コード体系；不正入力でサーバーがクラッシュしない。
"""


class DBError(Exception):
    """
    English: Base database error with MySQL-style code.
    Chinese: 带 MySQL 风格错误码的数据库异常基类。
    Japanese: MySQL 風エラーコード付き DB 例外基底クラス。
    """

    def __init__(self, code: int, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"[{code}] {message}")

    def format_for_wire(self) -> str:
        """Format for Wire Protocol response: 'ERROR [code] message'."""
        return f"[{self.code}] {self.message}"


# 1xxx - Syntax / Parse
class SQLSyntaxError(DBError):
    """1064 - SQL syntax error / SQL 语法错误"""

    def __init__(self, message: str = "SQL syntax error") -> None:
        super().__init__(1064, message)


class EmptySQLError(DBError):
    """1065 - Empty SQL / 空 SQL"""

    def __init__(self) -> None:
        super().__init__(1065, "Empty SQL statement")


# 2xxx - Schema / DDL
class UnknownTableError(DBError):
    """1146 - Table doesn't exist / 表不存在"""

    def __init__(self, table: str) -> None:
        super().__init__(1146, f"Table '{table}' doesn't exist")


class TableExistsError(DBError):
    """1050 - Table already exists / 表已存在"""

    def __init__(self, table: str) -> None:
        super().__init__(1050, f"Table '{table}' already exists")


class UnknownColumnError(DBError):
    """1054 - Unknown column / 未知列"""

    def __init__(self, column: str) -> None:
        super().__init__(1054, f"Unknown column '{column}'")


# 3xxx - Data / DML
class DuplicateKeyError(DBError):
    """1062 - Duplicate key / 主键重复"""

    def __init__(self, key: str = "") -> None:
        super().__init__(1062, f"Duplicate entry for key '{key}'")


class KeyNotFoundError(DBError):
    """1032 - Key not found / 主键未找到"""

    def __init__(self, key: str = "") -> None:
        super().__init__(1032, f"Key '{key}' not found")


# 4xxx - System
class UnknownDatabaseError(DBError):
    """1049 - Unknown database / 未知数据库"""

    def __init__(self, db: str = "") -> None:
        super().__init__(1049, f"Unknown database '{db}'")


class UnsupportedSQLError(DBError):
    """1109 - Unsupported SQL / 不支持的 SQL"""

    def __init__(self, hint: str = "") -> None:
        super().__init__(1109, f"Unsupported SQL: {hint}" if hint else "Unsupported SQL statement")
