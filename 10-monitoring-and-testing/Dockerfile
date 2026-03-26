FROM python:3.12-slim

WORKDIR /code

# 의존성 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 소스 코드 복사
COPY ./app /code/app

# 8000번 포트 노출
EXPOSE 8000

# uvicorn으로 FastAPI 실행
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
