CREATE TABLE IF NOT EXISTS gold.sources_daily
(
    EventDate Date,
    CounterID UInt32,
    TraficSourceID Int8,
    SearchEngineID UInt16,
    AdvEngineID UInt8,
    hits SimpleAggregateFunction(sum, UInt64),
    users AggregateFunction(uniq, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY EventDate
ORDER BY (CounterID, EventDate, TraficSourceID, SearchEngineID, AdvEngineID);

ALTER TABLE gold.sources_daily DROP PARTITION {day:String};

INSERT INTO gold.sources_daily
SELECT EventDate, CounterID, TraficSourceID, SearchEngineID, AdvEngineID, count(), uniqState(UserID)
FROM hits
WHERE EventDate = {day:String}
GROUP BY EventDate, CounterID, TraficSourceID, SearchEngineID, AdvEngineID;
