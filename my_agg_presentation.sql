-- Run against postgresql 13devel
-- create a table with some data
CREATE TABLE aggregate_test (
	id	SERIAL PRIMARY KEY,
	value FLOAT8 DEFAULT NULL
);

-- insert some reproducible random-ish values
SELECT setseed(.123);
INSERT INTO aggregate_test (value)
	SELECT
		CASE WHEN
			floor(random() * 10 + 1)::integer % 3 = 0
		THEN
			NULL
		ELSE
			(random() * 200) - 100
		END
	FROM
		generate_series(0, 199);

-- inspect the data
TABLE aggregate_test;

-- use a window function to express a cummulative sum
SELECT
  id,
  value,
  sum(value) OVER (ORDER BY id ASC) AS "Cummulative Sum"
FROM
        aggregate_test
ORDER BY
        id
ASC;

-- great, and the step?
WITH cte AS (SELECT
    id,
    value,
    sum(value) OVER (ORDER BY id ASC) AS "Cummulative Sum",
    LAG(value, 1) OVER (ORDER BY id ASC) AS lag
FROM
    aggregate_test
ORDER BY
    id
ASC)
SELECT id, value, "Cummulative Sum", value - lag AS step FROM cte;

-- transition function NOTE arrays are 1 based
CREATE OR REPLACE FUNCTION our_custom_transition(state float8[], value float8)
        RETURNS float8[] IMMUTABLE
        LANGUAGE plpgsql AS
$$
DECLARE
        cummulative_sum float8;
        difference              float8;
BEGIN
        IF value IS NULL THEN
                cummulative_sum := state[1];
                difference := state[3];
        ELSE
                cummulative_sum := state[1] + value;
                difference :=  value - state[3];
                state[3] := value;
        END IF;

        RETURN array[cummulative_sum, difference, state[3]];
END;
$$;

-- final function, i.e. pretty print
CREATE OR REPLACE FUNCTION our_custom_final(state float8[])
        RETURNS float8[] IMMUTABLE
        LANGUAGE plpgsql AS
$$
BEGIN
  RETURN array[state[1], state[2]];
END;
$$;

-- This is the aggregate
CREATE OR REPLACE aggregate our_custom_aggregate(float8) (
	initcond='{0.0, 0.0, 0.0}',
	sfunc = our_custom_transition,
	finalfunc = our_custom_final,
	stype = float8[]
);

-- And?
SELECT
	id,
	value,
	our_custom_aggregate(value) OVER (order by id asc)
FROM
	aggregate_test;

-- hack hack for getting columns back, a complex type would be better in this
-- case
CREATE OR REPLACE FUNCTION pretify (state float8[])
	RETURNS TABLE ("Cummulative Sum" float8, step float8)
	LANGUAGE sql AS
$$
	SELECT state[1], state[2];
$$
;

SELECT
	id,
	value,
	(pretify(our_custom_aggregate(value) OVER (order by id asc))).*
FROM
	aggregate_test;

