# 쿼리 서비스 Dockerfile (CQRS 읽기 모델)
FROM python:3.11-slim

WORKDIR /app

COPY query-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared /app/shared
COPY query-service/app/ /app/app/

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8005"]
