FROM python:3.13-slim
RUN pip install --no-cache-dir dagster==1.13.26 dagster-webserver==1.13.26 boto3 clickhouse-connect zstandard
ENV DAGSTER_HOME=/dagster_home
RUN mkdir -p $DAGSTER_HOME
WORKDIR /app
CMD ["dagster", "dev", "-m", "pipeline", "-h", "0.0.0.0", "-p", "3000"]
