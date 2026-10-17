CREATE TABLE IF NOT EXISTS gold.traffic_hourly
(
    EventDate Date,
    CounterID UInt32,
    Hour UInt8,
    hits SimpleAggregateFunction(sum, UInt64),
    users AggregateFunction(uniq, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY EventDate
ORDER BY (CounterID, EventDate, Hour);

ALTER TABLE gold.traffic_hourly DROP PARTITION {day:String};

INSERT INTO gold.traffic_hourly
SELECT EventDate, CounterID, toHour(EventTime), count(), uniqState(UserID)
FROM hits
WHERE EventDate = {day:String}
GROUP BY EventDate, CounterID, toHour(EventTime);
