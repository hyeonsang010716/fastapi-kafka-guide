# 주문 서비스 Dockerfile
FROM python:3.11-slim

WORKDIR /app

# 의존성 먼저 설치 (Docker 캐시 레이어 활용)
COPY order-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 공유 이벤트 모듈 복사
COPY shared /app/shared

# 애플리케이션 코드 복사
COPY order-service/app/ /app/app/

# 서비스 실행
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]
