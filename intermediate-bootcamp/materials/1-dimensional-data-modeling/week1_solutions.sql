--Task 1 — Creating the actors table.
CREATE TYPE films AS (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);
CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');
CREATE TABLE actors (
    actor TEXT,
    actorid TEXT,
    films films[],
    quality_class quality_class,
    is_active BOOLEAN,
    current_year INTEGER,
    PRIMARY KEY (actorid, current_year)
);

--Task 2 — Cumulative Table Generation Query.
INSERT INTO actors
WITH last_year AS(
    SELECT * FROM actors
    WHERE current_year = 1969
),
this_year AS (
    SELECT
        actor,
        actorid,
        year,
        ARRAY_AGG(ROW(film, votes, rating, filmid)::films) AS films,
        AVG(rating) AS avg_rating
    FROM actor_films
    WHERE year = 1970
    GROUP BY actor, actorid, year
)
SELECT
    COALESCE(ty.actor, ly.actor) AS actor,
    COALESCE(ty.actorid, ly.actorid) AS actorid,
    CASE
        WHEN ty.year IS NOT NULL THEN ly.films || ty.films
        ELSE ly.films
    END AS films,
    CASE
        WHEN ty.avg_rating > 8 THEN 'star'
        WHEN ty.avg_rating > 7 THEN 'good'
        WHEN ty.avg_rating > 6 THEN 'average'
        ELSE 'bad'
    END::quality_class AS quality_class,
    ty.year IS NOT NULL AS is_active,
    COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM last_year ly
FULL OUTER JOIN this_year ty ON ly.actorid = ty.actorid;


 --Task 3 — DDL for the actors_history_scd table.
 CREATE TABLE actors_history_scd (
    actor TEXT,
    actorid TEXT,
    quality_class quality_class,
    is_active BOOLEAN,
    start_date INTEGER,
    end_date INTEGER,
    is_current BOOLEAN,
    PRIMARY KEY (actorid, start_date)
);


--Task 4 — Backfill Query for actors_history_scd
INSERT INTO actors_history_scd
WITH with_previous AS (
    SELECT
        actor,
        actorid,
        current_year,
        quality_class,
        is_active,
        LAG(quality_class) OVER (PARTITION BY actorid ORDER BY current_year) AS prev_quality_class,
        LAG(is_active) OVER (PARTITION BY actorid ORDER BY current_year) AS prev_is_active
    FROM actors
),
with_change_indicator AS (
    SELECT *,
        CASE
            WHEN quality_class <> prev_quality_class THEN 1
            WHEN is_active <> prev_is_active THEN 1
            ELSE 0
        END AS change_indicator
    FROM with_previous
),
with_streak AS (
    SELECT *,
        SUM(change_indicator) OVER (PARTITION BY actorid ORDER BY current_year) AS streak_identifier
    FROM with_change_indicator
)
SELECT
    actor,
    actorid,
    quality_class,
    is_active,
    MIN(current_year) AS start_date,
    MAX(current_year) AS end_date,
    TRUE AS is_current
FROM with_streak
GROUP BY actor, actorid, quality_class, is_active, streak_identifier
ORDER BY actorid, start_date;


--Task 5 — Incremental Query for actors_history_scd.
INSERT INTO actors
WITH last_year AS (
    SELECT * FROM actors
    WHERE current_year = 1970
),
this_year AS (
    SELECT
        actor,
        actorid,
        year,
        ARRAY_AGG(ROW(film, votes, rating, filmid)::films) AS films,
        AVG(rating) AS avg_rating
    FROM actor_films
    WHERE year = 1971
    GROUP BY actor, actorid, year
)
SELECT
    COALESCE(ty.actor, ly.actor) AS actor,
    COALESCE(ty.actorid, ly.actorid) AS actorid,
    CASE
        WHEN ty.year IS NOT NULL THEN ly.films || ty.films
        ELSE ly.films
    END AS films,
    CASE
        WHEN ty.avg_rating > 8 THEN 'star'
        WHEN ty.avg_rating > 7 THEN 'good'
        WHEN ty.avg_rating > 6 THEN 'average'
        ELSE 'bad'
    END::quality_class AS quality_class,
    ty.year IS NOT NULL AS is_active,
    COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM last_year ly
FULL OUTER JOIN this_year ty ON ly.actorid = ty.actorid;