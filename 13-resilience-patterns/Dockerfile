FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./app ./app

# 기본은 client 앱을 띄우고, gateway 는 docker-compose 의 command 로 오버라이드한다.
CMD ["uvicorn", "app.client_main:app", "--host", "0.0.0.0", "--port", "8000"]
