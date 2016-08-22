CREATE SERVER mysql_fdw
FOREIGN DATA WRAPPER odbc_fdw
OPTIONS (
  odbc_DRIVER 'MySQL',
  odbc_SERVER 'localhost',
  odbc_PORT '3306',
  odbc_DATABASE 'fdw_tests',
  encoding 'iso88591'
);
CREATE USER MAPPING FOR postgres SERVER mysql_fdw
OPTIONS (
  odbc_UID 'root',
  odbc_PWD ''
);
IMPORT FOREIGN SCHEMA fdw_tests
FROM SERVER mysql_fdw
INTO public;
IMPORT FOREIGN SCHEMA fdw_tests
    FROM SERVER mysql_fdw
    INTO public
    OPTIONS(
      table 'query_mysql_test_table',
      sql_query 'select count(1) from mysql_test_table'
);
