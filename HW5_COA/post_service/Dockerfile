FROM python:3.9-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ADD https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh /wait-for-it.sh
RUN chmod +x /wait-for-it.sh
COPY . .
CMD ["/wait-for-it.sh", "kafka:9092", "--", "python", "server.py"]
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Сгенерировать Python-код из .proto
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. posts.proto

EXPOSE 50051
CMD ["python", "server.py"]