# 재고 서비스 Dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY inventory-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared /app/shared
COPY inventory-service/app/ /app/app/

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8003"]
