# stats_service/Dockerfile

FROM python:3.10-slim

WORKDIR /app

# 1) Копируем список зависимостей
COPY requirements.txt .

# 2) Устанавливаем их
RUN pip install --no-cache-dir -r requirements.txt

# 3) Копируем весь код сервиса
COPY . .

# 4) Запускаем сервер
CMD ["python", "server.py"]