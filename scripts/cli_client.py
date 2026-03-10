#!/usr/bin/env python3
"""
交互式 SQL 终端客户端。

English: Interactive SQL CLI client; connect to PyBPlus-DB server, run SQL.
Chinese: 交互式 SQL 终端；连接 PyBPlus-DB 服务器，执行 SQL。
Japanese: 対話型 SQL クライアント；PyBPlus-DB サーバーに接続し SQL を実行。
"""

import argparse
import struct
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

MSG_HEADER_LEN = 4


def send_query(sock: object, sql: str) -> str:
    """
    English: Send SQL via Wire Protocol [Length][Payload], receive response.
    Chinese: 按 Wire Protocol [Length][Payload] 发送 SQL，接收响应。
    Japanese: Wire Protocol [Length][Payload] で SQL を送信、応答を受信。
    """
    payload = sql.encode("utf-8")
    sock.sendall(struct.pack("<I", len(payload)) + payload)
    header = sock.recv(MSG_HEADER_LEN)
    if len(header) < MSG_HEADER_LEN:
        return ""
    length = struct.unpack("<I", header)[0]
    if length > 10 * 1024 * 1024:
        return "Response too large"
    data = b""
    while len(data) < length:
        chunk = sock.recv(length - len(data))
        if not chunk:
            break
        data += chunk
    return data.decode("utf-8", errors="replace")


def _format_table(lines: list[str]) -> str:
    """Format lines as aligned table (header + separator + rows)."""
    if not lines:
        return ""
    rows = [line.split("\t") for line in lines]
    widths = [max(len(str(r[i])) for r in rows) for i in range(len(rows[0]))]
    sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    out = [sep]
    for r in rows:
        out.append("|" + "|".join(f" {str(c):<{widths[i]}} " for i, c in enumerate(r)) + "|")
        if r is rows[0]:
            out.append(sep)
    out.append(sep)
    return "\n".join(out)


def run_client(host: str, port: int) -> None:
    """
    English: Interactive loop: read SQL, send, display result.
    Chinese: 交互循环：读取 SQL、发送、显示结果。
    Japanese: 対話ループ：SQL を読取り、送信、結果を表示。
    """
    import socket

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host, port))
    except OSError as e:
        print(f"Connection failed: {e}", file=sys.stderr)
        sys.exit(1)

    print("PyBPlus-DB CLI (type QUIT to exit)")
    print("-" * 40)

    buffer = ""
    while True:
        try:
            if buffer:
                prompt = "    -> "
            else:
                prompt = "pybplus> "
            line = input(prompt)
        except EOFError:
            break
        buffer += (" " + line) if buffer else line
        if ";" in buffer or buffer.strip().upper() in ("QUIT", "EXIT", "BYE"):
            sql = buffer.strip().rstrip(";").strip()
            buffer = ""
            if not sql or sql.upper() in ("QUIT", "EXIT", "BYE"):
                if sql:
                    break
                continue
            resp = send_query(sock, sql)
            if not resp:
                print("(connection closed)")
                break
            parts = resp.split("\n")
            if len(parts) < 2:
                print(resp)
                continue
            status, message = parts[0], parts[1]
            if status == "ERROR":
                print(f"Error: {message}")
                continue
            if status == "OK":
                if len(parts) > 2:
                    table_lines = parts[2:]
                    if table_lines:
                        print(_format_table(table_lines))
                print(f"\n{message}")
    sock.close()
    print("Bye.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="PyBPlus-DB interactive SQL client (like mysql -u root)"
    )
    parser.add_argument("-H", "--host", dest="host", default="127.0.0.1", help="Server host")
    parser.add_argument("-P", "--port", dest="port", type=int, default=8765, help="Server port")
    args = parser.parse_args()
    run_client(args.host, args.port)


if __name__ == "__main__":
    main()
