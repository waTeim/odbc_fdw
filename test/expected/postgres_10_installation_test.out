CREATE SERVER postgres_fdw
FOREIGN DATA WRAPPER odbc_fdw
OPTIONS (
  odbc_DRIVER 'PostgreSQL Unicode',
  odbc_SERVER 'localhost',
  odbc_PORT '5432',
  odbc_DATABASE 'fdw_tests'
);
CREATE USER MAPPING FOR postgres SERVER postgres_fdw
OPTIONS (
  odbc_UID 'postgres',
  odbc_PWD ''
);
IMPORT FOREIGN SCHEMA public
    FROM SERVER postgres_fdw
    INTO public
    OPTIONS(
      table 'query_postgres_test_table',
      odbc_BoolsAsChar '0',
      odbc_ByteaAsLongVarBinary '1',
      sql_query 'select count(1) from postgres_test_table'
);
IMPORT FOREIGN SCHEMA public
    FROM SERVER postgres_fdw
    INTO public
    OPTIONS(
      table 'postgres_test_table',
      odbc_BoolsAsChar '0',
      odbc_ByteaAsLongVarBinary '1'
);
