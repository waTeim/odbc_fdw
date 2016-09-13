SELECT * FROM mysql_test_table;
SELECT * FROM query_mysql_test_table;
SELECT * FROM ODBCTablesList('mysql_fdw', 1);
SELECT * FROM ODBCTableSize('mysql_fdw', 'mysql_test_table');
SELECT * FROM ODBCQuerySize('mysql_fdw', 'select * from mysql_test_table');