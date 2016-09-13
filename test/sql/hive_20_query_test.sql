SELECT * FROM hive_test_table;
SELECT * FROM query_hive_test_table;
SELECT * FROM ODBCTablesList('hive_fdw', 1);
SELECT * FROM ODBCTableSize('hive_fdw', 'hive_test_table');
SELECT * FROM ODBCQuerySize('hive_fdw', 'select * from hive_test_table');