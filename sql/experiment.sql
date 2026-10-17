DROP TABLE IF EXISTS hits_baseline;
CREATE TABLE hits_baseline AS raw.hits ENGINE = MergeTree PARTITION BY EventDate ORDER BY EventTime;
INSERT INTO hits_baseline SELECT * FROM hits;

DROP TABLE IF EXISTS hits_sorted;
CREATE TABLE hits_sorted AS raw.hits ENGINE = MergeTree PARTITION BY EventDate ORDER BY (CounterID, intHash32(UserID), EventTime);
INSERT INTO hits_sorted SELECT * FROM hits;

OPTIMIZE TABLE hits_baseline FINAL;
OPTIMIZE TABLE hits_sorted FINAL;
OPTIMIZE TABLE hits FINAL;

SELECT
    table,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed,
    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio
FROM system.columns
WHERE database = currentDatabase() AND table IN ('hits_baseline', 'hits_sorted', 'hits')
GROUP BY table
ORDER BY sum(data_compressed_bytes) DESC
FORMAT PrettyCompactMonoBlock;

SELECT
    name,
    formatReadableSize(sumIf(data_compressed_bytes, table = 'hits_baseline')) AS baseline,
    formatReadableSize(sumIf(data_compressed_bytes, table = 'hits_sorted')) AS sorted,
    formatReadableSize(sumIf(data_compressed_bytes, table = 'hits')) AS hits,
    anyIf(compression_codec, table = 'hits') AS codec
FROM system.columns
WHERE database = currentDatabase() AND table IN ('hits_baseline', 'hits_sorted', 'hits')
GROUP BY name
ORDER BY sumIf(data_compressed_bytes, table = 'hits_baseline') DESC
LIMIT 25
FORMAT PrettyCompactMonoBlock;
