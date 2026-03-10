# PyBPlus-DBEngine 生产级 Docker 镜像
# Production Docker image for PyBPlus-DBEngine
# PyBPlus-DBEngine 本番 Docker イメージ

FROM python:3.10-slim

WORKDIR /app

COPY pyproject.toml .
COPY src/ src/
COPY scripts/ scripts/

RUN pip install --no-cache-dir -e .

# 数据目录
VOLUME /data

ENV PYTHONUNBUFFERED=1

EXPOSE 8765

# 默认：使用 /data 作为数据目录，支持恢复与 DDL
CMD ["python", "scripts/run_server.py", "-H", "0.0.0.0", "-P", "8765", "-d", "/data"]
