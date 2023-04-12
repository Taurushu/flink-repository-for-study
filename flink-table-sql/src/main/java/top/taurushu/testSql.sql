-- CREATE TABLE inputFileTable
-- (
--     `name` String,
--     `url`  String,
--     `time` BIGINT,
--     `time_ltz` AS TO_TIMESTAMP_LTZ(`time`, 3),
--     WaterMark For `time_ltz` as `time_ltz` - Interval '5' second
-- ) WITH (
--       'connector' = 'filesystem',
--       'path' = 'input/input.txt',
--       'format' = 'csv'
--       );
--
--
-- SELECT `name`,
--        window_start,
--        window_end,
--        COUNT(1) as cn
-- FROM TABLE(TUMBLE(TABLE inputFileTable, DESCRIPTOR(`time_ltz`), INTERVAL '1' HOURS))
-- GROUP BY `name`, window_start, window_end;
--
--
--
-- SELECT `name`, `cn`, window_start, window_end, `rank`
-- FROM (SELECT `name`,
--              `cn`,
--              window_start,
--              window_end,
--              ROW_NUMBER() OVER(PARTITION BY window_start, window_end ORDER BY `cn` desc) as `rank`
--       FROM uvCount)
-- WHERE `rank` <= 2;
--
--
-- SELECT `name`, `cn`, window_start, window_end, `rank`
-- FROM uvCount u, inputFileTable v
-- WHERE u.`name` = v.`name` AND o.order_time BETWEEN s.ship_time - INTERVAL '4' HOUR AND s.ship_time;
--

CREATE TABLE inputKafkaSource
(
    `name` String,
    `url`  String,
    `time` BIGINT,
    `timestamp` AS TO_TIMESTAMP_LTZ(`time`, 3),
    WaterMark For `timestamp` as `timestamp` - Interval '1' second
) WITH (
      'connector' = 'kafka',
      'topic' = 'flink-generate-topic',
      'properties.bootstrap.servers' = 'node1:9092',
      'properties.group.id' = 'testGroup',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'csv'
      );

CREATE TABLE reallyAOutPutKafkaSource
(
    `name`      String,
    `url`       String,
    `time`      BIGINT,
    `timestamp` TIMESTAMP(3)
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'flink-generate-topic-really',
      'properties.bootstrap.servers' = 'node1:9092',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'csv'
      );

CREATE TABLE outputKafkaSource
(
    `name`      String,
    `url`       String,
    `time`      BIGINT,
    `timestamp` TIMESTAMP(3)
) WITH (
      'connector' = 'print'
      );

insert into really1OutPutKafkaSource select `name`, `url`, `time`, `timestamp` from inputKafkaSource;