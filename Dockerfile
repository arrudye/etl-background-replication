FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY etl_script.py .
COPY .env .env

CMD ["python", "etl_script.py"]