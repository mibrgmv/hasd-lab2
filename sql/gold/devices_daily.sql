CREATE TABLE IF NOT EXISTS gold.devices_daily
(
    EventDate Date,
    CounterID UInt32,
    OS UInt8,
    UserAgent UInt8,
    IsMobile UInt8,
    hits SimpleAggregateFunction(sum, UInt64),
    users AggregateFunction(uniq, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY EventDate
ORDER BY (CounterID, EventDate, OS, UserAgent, IsMobile);

ALTER TABLE gold.devices_daily DROP PARTITION {day:String};

INSERT INTO gold.devices_daily
SELECT EventDate, CounterID, OS, UserAgent, IsMobile, count(), uniqState(UserID)
FROM hits
WHERE EventDate = {day:String}
GROUP BY EventDate, CounterID, OS, UserAgent, IsMobile;
