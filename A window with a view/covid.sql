--
-- https://blog.hagander.net/fdws-curl-and-limit-247/
--
-- CREATE EXTENSION file_fdw;
-- 
-- CREATE SERVER curly FOREIGN DATA WRAPPER file_fdw;
-- 
-- CREATE FOREIGN TABLE _rawdata (
--  daterep text not null,
--  day int not null,
--  month int not null,
--  year int not null,
--  cases int not null,
--  deaths int not null,
--  countries text not null,
--  geoid text not null,
--  countrycode text null,
--  popdata int null,
--  continent text not null,
--  cumulative14days float null
-- )
-- SERVER curly
-- OPTIONS (
--  PROGRAM 'curl -s
-- https://opendata.ecdc.europa.eu/covid19/casedistribution/csv/',
--  FORMAT 'csv',
--  HEADER 'on'
-- );
-- 
-- CREATE MATERIALIZED VIEW covid AS
--  SELECT to_date(daterep, 'dd/mm/yyyy') AS date,
--         cases,
--         deaths,
--         countries,
--         geoid,
--         countrycode,
--         popdata,
--         continent,
--         cumulative14days
--  FROM _rawdata;
--
-- CREATE UNIQUE INDEX idx_covid_datecountry ON covid(date, countrycode);
-- CREATE INDEX idx_covid_countrycode ON covid(countrycode);
-- CREATE INDEX idx_covid_countries ON covid(countries);
--

--
-- Number of cases, deaths in Europe today.
-- Most cases and deaths come first
--
SELECT
	date,
	countries,
	cases,
	deaths
FROM
	covid
WHERE
	continent = 'Europe' AND
	date = NOW()::date
ORDER BY
	(cases, deaths) DESC
;

--
-- Number of cases, deaths in Europe Last 14 days.
-- Most cases and deaths come first
--
-- postgres=# select INTERVAL '14 days', INTERVAL '2 weeks';
--  interval | interval
-- ----------+----------
--  14 days  | 14 days
-- (1 row)
SELECT
	date,
	countries,
	cases,
	deaths
FROM
	covid
WHERE
	continent = 'Europe' AND
	date >= NOW() - INTERVAL '2 weeks'
ORDER BY
	(date, cases, deaths) DESC
;

--
-- Number of cases, deaths
-- Within Europe Last 14 days.
-- Most cases first
--
SELECT
    date,
    countries,
    cases,
    deaths
FROM
    covid
WHERE
    continent = 'Europe' AND
    date >= NOW() - INTERVAL '2 weeks'
ORDER BY
    cases DESC
;

--
-- Number of cases placement, cases, deaths
-- Within Europe Last 14 days.
-- Highest placement first
--
SELECT
    date,
    countries,
    RANK() OVER (ORDER BY cases DESC) placement,
    cases,
    deaths
FROM
    covid
WHERE
    continent = 'Europe' AND
    date >= NOW() - INTERVAL '2 weeks'
ORDER BY
    placement ASC
;

--
-- Number of cases placement per day, cases, deaths
-- Within Europe Last 14 days.
-- Highest placement per day first
--
SELECT
    date,
    countries,
    RANK() OVER (PARTITION BY date ORDER BY cases DESC) placement,
    cases,
    deaths
FROM
    covid
WHERE
    continent = 'Europe' AND
    date >= NOW() - INTERVAL '2 weeks'
ORDER BY
    date DESC,
	placement ASC
;

--
-- Number of cases placement per day, cases, daily cases diff per country, deaths
-- Within Europe Last 14 days.
-- Highest placement per day first
--
SELECT
    date,
    countries,
    RANK() OVER (PARTITION BY date ORDER BY cases DESC) placement,
    cases - lag(cases) OVER (PARTITION BY countries ORDER BY date DESC) diff,
    cases,
    deaths
FROM
    covid
WHERE
    continent = 'Europe' AND
    date >= NOW() - INTERVAL '2 weeks'
ORDER BY
    date DESC,
	placement ASC
;

--
-- Number of cases placement per day, cases, daily cases diff per country, deaths
-- Within Europe Last 14 days.
-- Show only the 5 top placed, Sweden and Greece
-- Highest placement per day first
--
-- XXX: This fails because the column placement does not exist, this is
-- something that can not be evaluated since we are basically asking to execute
-- a window function in a where clause
-- SELECT
--     date,
--     countries,
--     RANK() OVER (PARTITION BY date ORDER BY cases DESC) placement,
--     cases - lag(cases) OVER (PARTITION BY countries ORDER BY date ASC) diff,
--     cases,
--     deaths
-- FROM
--     covid
-- WHERE
--     continent = 'Europe' AND
--     date >= NOW() - INTERVAL '2 weeks' AND
--     (
--         placement <= 5 OR
--         countries IN ('Sweden', 'Greece')
--     )
-- ORDER BY
--     date DESC,
--     placement ASC
-- ;
-- Instead of the above we need to use a subquery or a CTE
SELECT
    *
FROM
    (SELECT
        date,
        countries,
        RANK() OVER (PARTITION BY date ORDER BY cases DESC) placement,
        cases - lag(cases) OVER (PARTITION BY countries ORDER BY date ASC) diff,
        cases,
        deaths
    FROM
        covid
    WHERE
        continent = 'Europe' AND
        date >= NOW() - INTERVAL '2 weeks'
    ) s
WHERE
    placement <= 5 OR
    countries IN ('Sweden', 'Greece')
ORDER BY
    date DESC,
    placement ASC
;

--
-- Number of cases placement per day, cases, daily cases diff per country, most cases placement per country, deaths
-- Within Europe Last 14 days.
-- Show only the 5 top placed, Sweden and Greece
-- Highest placement per day first
--
-- XXX: Note that now the WHERE clause in the subquery has been reduced,
-- otherwise I would not have been able to take the total most cases placement
-- since the result set would have been limited by the where clause.
-- But why did it work until now? Because all the questions where relative to
-- that set, either worst by day or placement by day or lag.
SELECT
    *
FROM
    (SELECT
        date,
        countries,
        RANK() OVER (PARTITION BY date ORDER BY cases DESC) daily_placement,
        RANK() OVER (PARTITION BY countries ORDER BY cases DESC) most_cases_per_country_placement,
        cases - LAG(cases, 1) OVER (PARTITION BY countries ORDER BY date ASC) diff,
        cases,
        deaths
    FROM
        covid
    WHERE
        continent = 'Europe'
    ) s
WHERE
    date >= NOW() - INTERVAL '2 weeks' AND
    (
        daily_placement <= 5 OR
        countries IN ('Sweden', 'Greece')
    )
ORDER BY
    date DESC,
    daily_placement ASC
;

--
-- Number of cases placement per day, cases, daily cases diff per country,
-- most cases placement per country, cases diff from overall country max, deaths
-- Within Europe Last 14 days.
-- Show only the 5 top placed, Sweden and Greece
-- Highest placement per day first
--
SELECT
    *
FROM
    (SELECT
        date,
        countries,
        RANK() OVER (PARTITION BY date ORDER BY cases DESC) daily_placement,
        RANK() OVER (PARTITION BY countries ORDER BY cases DESC) worst_day_placement,
        cases - LAG(cases, 1) OVER (PARTITION BY countries ORDER BY date ASC) daily_diff,
        cases - LAST_VALUE(cases) OVER (PARTITION BY countries ORDER BY cases ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) diff_from_max,
        cases,
        deaths
    FROM
        covid
    WHERE
        continent = 'Europe'
    ) s
WHERE
    date >= NOW() - INTERVAL '2 weeks' AND
    (
        daily_placement <= 5 OR
        countries IN ('Sweden', 'Greece')
    )
ORDER BY
    date DESC,
    daily_placement ASC
;

--
-- Number of cases placement per day, cases, daily cases diff per country,
-- most cases placement per country,
-- cases diff from overall country max, deaths
-- Within Europe Last 14 days.
-- Show only the 5 top placed, Sweden and Greece
-- Highest placement per day first
-- XXX: This is not really tested yet
SELECT
	date,
	countries,
	LAST_VALUE() OVER (PARTITION BY countries ORDER BY cases DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) rows,
	LAST_VALUE() OVER (PARTITION BY countries ORDER BY cases DESC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) range,
	cases
FROM
	covid
WHERE
	continent = 'Europe' AND 
	date >= NOW() - INTERVAL '1 day'
;

