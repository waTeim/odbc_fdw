CREATE SERVER hive_fdw FOREIGN DATA WRAPPER odbc_fdw
  OPTIONS (
    odbc_DRIVER '${driver}',
    odbc_HOST '${host}',
    odbc_PORT '${port}'
);
CREATE USER MAPPING FOR postgres SERVER hive_fdw;
IMPORT FOREIGN SCHEMA fdw_tests
  FROM SERVER hive_fdw
  INTO public
  OPTIONS(
    schema ''
);
IMPORT FOREIGN SCHEMA fdw_tests
  FROM SERVER hive_fdw
  INTO public
  OPTIONS(
    table 'query_hive_test_table',
    sql_query 'select count(1) from hive_test_table',
    schema ''
);