FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY data_generator.py .
COPY bronze_consumer.py .
COPY data/ ./data/

CMD ["python", "data_generator.py"]
