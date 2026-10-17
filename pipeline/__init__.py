import dagster as dg

from pipeline.assets import hits, raw_hits
from pipeline.resources import S3, Batches, ClickHouse

defs = dg.Definitions(
    assets=[raw_hits, hits],
    resources={
        "batches": Batches(root=dg.EnvVar("BATCHES_DIR")),
        "s3": S3(
            endpoint=dg.EnvVar("S3_ENDPOINT"),
            region=dg.EnvVar("S3_REGION"),
            bucket=dg.EnvVar("S3_BUCKET"),
            access_key=dg.EnvVar("GARAGE_ACCESS_KEY"),
            secret_key=dg.EnvVar("GARAGE_SECRET_KEY"),
        ),
        "clickhouse": ClickHouse(host=dg.EnvVar("CLICKHOUSE_HOST")),
    },
)
