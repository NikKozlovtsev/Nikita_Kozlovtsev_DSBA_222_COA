FROM python:3.9-slim
WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Сгенерировать Python-код из .proto
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. posts.proto

EXPOSE 50051
CMD ["python", "server.py"]