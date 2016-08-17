DROP TABLE IF EXISTS postgres_test_table;
CREATE TABLE postgres_test_table (
    id integer PRIMARY KEY,
    varchar_example varchar(40),
    text_example text,
    integer_example integer,
    numeric_example numeric,
    timestamp_example   timestamp,
    boolean_example boolean
);
INSERT INTO postgres_test_table VALUES (1, 'example', 'example', 100, 10.12, '2016-01-01 00:00:00', true);