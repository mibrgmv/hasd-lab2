from pathlib import Path

import boto3
import clickhouse_connect
import dagster as dg
from botocore.config import Config

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"


class Batches(dg.ConfigurableResource):
    root: str

    def path(self, day):
        return Path(self.root) / f"{day}.tsv"


class S3(dg.ConfigurableResource):
    endpoint: str
    region: str
    bucket: str
    access_key: str
    secret_key: str

    def client(self):
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            region_name=self.region,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            config=Config(
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
            ),
        )


class ClickHouse(dg.ConfigurableResource):
    host: str

    def client(self):
        return clickhouse_connect.get_client(host=self.host)

    def run_file(self, name):
        client = self.client()
        for statement in (SQL_DIR / name).read_text().split(";"):
            if statement.strip():
                client.command(statement)

    def command(self, query, **parameters):
        self.client().command(query, parameters=parameters)

    def row(self, query, **parameters):
        return self.client().query(query, parameters=parameters).result_rows[0]

    def scalar(self, query, **parameters):
        return self.row(query, **parameters)[0]
