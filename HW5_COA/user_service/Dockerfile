FROM python:3.9-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# grab the wait-for-it.sh helper
ADD https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh /wait-for-it.sh
RUN chmod +x /wait-for-it.sh
COPY . .
CMD ["/wait-for-it.sh", "kafka:9092", "--", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5001"]
# Установим netcat, если нужно
RUN apt-get update \
 && apt-get install -y --no-install-recommends netcat-openbsd \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Копируем всё, включая wait-for-it.sh
COPY . .

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5001"]
EXPOSE 5001
# CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5001"]