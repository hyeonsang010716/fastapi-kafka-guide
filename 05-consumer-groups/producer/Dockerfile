FROM python:3.12-slim

WORKDIR /code

# 의존성 파일 복사 후 설치 (레이어 캐싱 활용)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 소스 복사
COPY ./app ./app

# uvicorn 실행 (host 0.0.0.0으로 컨테이너 외부 접근 허용)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
