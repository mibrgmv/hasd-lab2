CREATE TABLE IF NOT EXISTS gold.traffic_daily
(
    EventDate Date,
    CounterID UInt32,
    hits SimpleAggregateFunction(sum, UInt64),
    users AggregateFunction(uniq, UInt64),
    mobile_hits SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY EventDate
ORDER BY (CounterID, EventDate);

ALTER TABLE gold.traffic_daily DROP PARTITION {day:String};

INSERT INTO gold.traffic_daily
SELECT EventDate, CounterID, count(), uniqState(UserID), countIf(IsMobile = 1)
FROM hits
WHERE EventDate = {day:String}
GROUP BY EventDate, CounterID;
