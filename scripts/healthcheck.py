#!/usr/bin/env python3
"""Docker healthcheck: verify server port 8765 is reachable."""
import socket
import sys

def main() -> int:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        s.connect(("127.0.0.1", 8765))
        s.close()
        return 0
    except Exception:
        return 1


if __name__ == "__main__":
    sys.exit(main())
