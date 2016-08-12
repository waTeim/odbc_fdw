CREATE EXTENSION odbc_fdw;

CREATE SERVER mysql_aws
FOREIGN DATA WRAPPER odbc_fdw
OPTIONS (
  odbc_DRIVER 'MySQL',
  odbc_SERVER '<%= mysql_server %>',
  odbc_DATABASE '<%= mysql_database %>',
  encoding '<%= mysql_server %>'
);

CREATE USER MAPPING FOR postgres SERVER mysql_aws
OPTIONS (
  odbc_UID '<%= mysql_username %>',
  odbc_PWD '<%= mysql_password %>'
);

IMPORT FOREIGN SCHEMA fdw_tests
FROM SERVER mysql_aws
INTO public;

SELECT * FROM odbc_test_table;