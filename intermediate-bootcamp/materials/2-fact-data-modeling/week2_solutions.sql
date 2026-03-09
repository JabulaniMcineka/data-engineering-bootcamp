
--Query 1 — Deduplicating game_details
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY game_id, team_id, player_id
            ORDER BY game_id
        ) AS row_num
    FROM game_details
)
SELECT * FROM deduped
WHERE row_num = 1;

--Query 2 — DDL for user_devices_cumulated table
CREATE TABLE user_devices_cumulated (
    user_id NUMERIC,
    browser_type TEXT,
    device_activity_datelist DATE[],
    date DATE,
    PRIMARY KEY (user_id, browser_type, date)
);


-- Cumulative Query to generate device_activity_datelist

INSERT INTO user_devices_cumulated
WITH yesterday AS (
    SELECT * FROM user_devices_cumulated
    WHERE date = DATE('2023-01-30')
),
today AS (
    SELECT
        e.user_id,
        d.browser_type,
        DATE(CAST(e.event_time AS TIMESTAMP)) AS today_date
    FROM events e
    JOIN devices d ON e.device_id = d.device_id
    WHERE DATE(CAST(e.event_time AS TIMESTAMP)) = DATE('2023-01-31')
    AND e.user_id IS NOT NULL
    GROUP BY e.user_id, d.browser_type, DATE(CAST(e.event_time AS TIMESTAMP))
)
SELECT
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(t.browser_type, y.browser_type) AS browser_type,
    CASE
        WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.today_date]
        WHEN t.today_date IS NOT NULL THEN y.device_activity_datelist || ARRAY[t.today_date]
        ELSE y.device_activity_datelist
    END AS device_activity_datelist,
    COALESCE(t.today_date, y.date + 1) AS date
FROM today t
FULL OUTER JOIN yesterday y
    ON t.user_id = y.user_id
    AND t.browser_type = y.browser_type;

--Query 4 — datelist_int Generation Query

    WITH users AS (
    SELECT * FROM user_devices_cumulated
    WHERE date = DATE('2023-01-31')
),
date_list_int AS (
    SELECT
        user_id,
        browser_type,
        CAST(
            SUM(
                CASE
                    WHEN UNNEST(device_activity_datelist) = date - interval '0 days' THEN POW(2, 0)
                    WHEN UNNEST(device_activity_datelist) = date - interval '1 days' THEN POW(2, 1)
                    WHEN UNNEST(device_activity_datelist) = date - interval '2 days' THEN POW(2, 2)
                    WHEN UNNEST(device_activity_datelist) = date - interval '3 days' THEN POW(2, 3)
                    WHEN UNNEST(device_activity_datelist) = date - interval '4 days' THEN POW(2, 4)
                    WHEN UNNEST(device_activity_datelist) = date - interval '5 days' THEN POW(2, 5)
                    WHEN UNNEST(device_activity_datelist) = date - interval '6 days' THEN POW(2, 6)
                    ELSE 0
                END
            ) AS BIGINT
        ) AS datelist_int
    FROM users
    GROUP BY user_id, browser_type, date
)
SELECT
    user_id,
    browser_type,
    datelist_int,
    BIT_COUNT(datelist_int::bit(7)) AS days_active_last_week
FROM date_list_int;


--Query 5 — DDL for hosts_cumulated table
CREATE TABLE hosts_cumulated (
    host TEXT,
    host_activity_datelist DATE[],
    date DATE,
    PRIMARY KEY (host, date)
);


--Query 6 — Incremental Query to generate host_activity_datelist

INSERT INTO hosts_cumulated
WITH yesterday AS (
    SELECT * FROM hosts_cumulated
    WHERE date = DATE('2023-01-30')
),
today AS (
    SELECT
        host,
        DATE(CAST(event_time AS TIMESTAMP)) AS today_date
    FROM events
    WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
    AND host IS NOT NULL
    GROUP BY host, DATE(CAST(event_time AS TIMESTAMP))
)
SELECT
    COALESCE(t.host, y.host) AS host,
    CASE
        WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.today_date]
        WHEN t.today_date IS NOT NULL THEN y.host_activity_datelist || ARRAY[t.today_date]
        ELSE y.host_activity_datelist
    END AS host_activity_datelist,
    COALESCE(t.today_date, y.date + 1) AS date
FROM today t
FULL OUTER JOIN yesterday y ON t.host = y.host;


---Query 7 — DDL for host_activity_reduced table
CREATE TABLE host_activity_reduced (
    month DATE,
    host TEXT,
    hit_array INTEGER[],
    unique_visitors INTEGER[],
    PRIMARY KEY (month, host)
);

--Query 8 — Incremental Query to load host_activity_reduced

INSERT INTO host_activity_reduced
WITH daily_aggregate AS (
    SELECT
        host,
        DATE(CAST(event_time AS TIMESTAMP)) AS date,
        COUNT(1) AS hit_count,
        COUNT(DISTINCT user_id) AS unique_visitors
    FROM events
    WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
    AND host IS NOT NULL
    GROUP BY host, DATE(CAST(event_time AS TIMESTAMP))
),
yesterday_array AS (
    SELECT * FROM host_activity_reduced
    WHERE month = DATE_TRUNC('month', DATE('2023-01-31'))
)
SELECT
    COALESCE(
        ya.month,
        DATE_TRUNC('month', da.date)
    ) AS month,
    COALESCE(da.host, ya.host) AS host,
    CASE
        WHEN ya.hit_array IS NOT NULL THEN
            ya.hit_array || ARRAY[COALESCE(da.hit_count, 0)]
        ELSE
            ARRAY_FILL(0, ARRAY[EXTRACT(DAY FROM da.date)::INT - 1]) || ARRAY[COALESCE(da.hit_count, 0)]
    END AS hit_array,
    CASE
        WHEN ya.unique_visitors IS NOT NULL THEN
            ya.unique_visitors || ARRAY[COALESCE(da.unique_visitors, 0)]
        ELSE
            ARRAY_FILL(0, ARRAY[EXTRACT(DAY FROM da.date)::INT - 1]) || ARRAY[COALESCE(da.unique_visitors, 0)]
    END AS unique_visitors
FROM daily_aggregate da
FULL OUTER JOIN yesterday_array ya ON da.host = ya.host
ON CONFLICT (month, host)
DO UPDATE SET
    hit_array = EXCLUDED.hit_array,
    unique_visitors = EXCLUDED.unique_visitors;

