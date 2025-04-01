FROM python:3.9-slim
WORKDIR /app

# Установим netcat, если нужно
RUN apt-get update && apt-get install -y netcat

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Копируем всё, включая wait-for-it.sh
COPY . .

RUN chmod +x /app/wait-for-it.sh
EXPOSE 5001
# CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5001"]