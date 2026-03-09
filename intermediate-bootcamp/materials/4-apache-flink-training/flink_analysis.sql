-- Q1: Average number of web events per session on Tech Creator
SELECT
    host,
    AVG(event_count) AS avg_events_per_session
FROM sessionized_events
WHERE host = 'zachwilson.techcreator.io'
GROUP BY host;

-- Q2: Compare results between different hosts
SELECT
    host,
    COUNT(*) AS total_sessions,
    AVG(event_count) AS avg_events_per_session,
    MIN(event_count) AS min_events,
    MAX(event_count) AS max_events
FROM sessionized_events
WHERE host IN (
    'zachwilson.techcreator.io',
    'zachwilson.tech',
    'lulu.techcreator.io'
)
GROUP BY host
ORDER BY avg_events_per_session DESC;
