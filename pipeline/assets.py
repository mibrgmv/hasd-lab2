import tempfile

import dagster as dg
import zstandard

from pipeline.resources import S3, Batches, ClickHouse

RAW_KEY = "raw/date={day}/hits.tsv.zst"
ZSTD_LEVEL = 9
CHUNK_SIZE = 16 << 20

days = dg.DailyPartitionsDefinition(start_date="2014-03-17", end_date="2014-03-24")


@dg.asset(partitions_def=days)
def raw_hits(
    context: dg.AssetExecutionContext, batches: Batches, s3: S3, clickhouse: ClickHouse
) -> dg.MaterializeResult:
    day = context.partition_key
    key = RAW_KEY.format(day=day)
    rows = 0

    with open(batches.path(day), "rb") as src, tempfile.TemporaryFile() as dst:
        with zstandard.ZstdCompressor(level=ZSTD_LEVEL, threads=-1).stream_writer(dst, closefd=False) as writer:
            for chunk in iter(lambda: src.read(CHUNK_SIZE), b""):
                rows += chunk.count(b"\n")
                writer.write(chunk)
        tsv_bytes = src.tell()
        zst_bytes = dst.tell()
        dst.seek(0)
        s3.client().upload_fileobj(dst, s3.bucket, key)

    clickhouse.run_file("raw.sql")
    loaded = clickhouse.scalar("SELECT count() FROM raw.hits WHERE date = {day:String}", day=day)
    if loaded != rows:
        raise dg.Failure(f"{key}: {rows} rows in batch, {loaded} rows in raw.hits")

    return dg.MaterializeResult(
        metadata={
            "s3_key": key,
            "rows": rows,
            "tsv_bytes": tsv_bytes,
            "zst_bytes": zst_bytes,
            "ratio": round(tsv_bytes / zst_bytes, 2),
        }
    )


@dg.asset(partitions_def=days, deps=[raw_hits])
def hits(context: dg.AssetExecutionContext, clickhouse: ClickHouse) -> dg.MaterializeResult:
    day = context.partition_key

    clickhouse.run_file("hits.sql")
    clickhouse.command(f"ALTER TABLE hits DROP PARTITION '{day}'")
    clickhouse.command("INSERT INTO hits SELECT * FROM raw.hits WHERE date = {day:String}", day=day)

    expected = clickhouse.row("SELECT count(), sum(cityHash64(*)) FROM raw.hits WHERE date = {day:String}", day=day)
    actual = clickhouse.row("SELECT count(), sum(cityHash64(*)) FROM hits WHERE EventDate = {day:String}", day=day)
    if actual != expected:
        raise dg.Failure(f"hits {day}: (rows, fingerprint) {actual} != raw.hits {expected}")

    parts, disk_bytes, data_bytes = clickhouse.row(
        "SELECT count(), sum(bytes_on_disk), sum(data_uncompressed_bytes) FROM system.parts "
        "WHERE database = currentDatabase() AND table = 'hits' AND partition = {day:String} AND active",
        day=day,
    )

    return dg.MaterializeResult(
        metadata={
            "rows": actual[0],
            "fingerprint": str(actual[1]),
            "parts": parts,
            "disk_bytes": disk_bytes,
            "ratio": round(data_bytes / disk_bytes, 2),
        }
    )
