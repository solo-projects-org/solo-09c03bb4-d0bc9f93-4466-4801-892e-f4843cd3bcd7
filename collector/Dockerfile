FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY __init__.py ./collector/__init__.py
COPY config.py ./collector/config.py
COPY db.py ./collector/db.py
COPY storage.py ./collector/storage.py
COPY transfer.py ./collector/transfer.py
COPY app.py ./collector/app.py

ENV PYTHONUNBUFFERED=1 \
    COLLECTOR_DATA_DIR=/data

EXPOSE 8000
# 单 worker：跨线程的 SQLite 连接与文件锁语义最简单；
# 高可用由 compose 中的两个对等实例提供，二者共享数据卷。
CMD ["uvicorn", "collector.app:app", "--host", "0.0.0.0", "--port", "8000"]
