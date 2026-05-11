FROM python:3.11-slim

RUN pip install --no-cache-dir psycopg2-binary==2.9.9

WORKDIR /app
COPY outbreak_prediction.py /app/outbreak_prediction.py

CMD ["python", "/app/outbreak_prediction.py", "--live"]
