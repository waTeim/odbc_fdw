-- This fixture is going to be executed with TSQL
use master
go
if exists(select * from sys.databases where name = 'fdw_tests')
 ALTER DATABASE fdw_tests SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
 drop database fdw_tests;
go
create database fdw_tests;
go
use fdw_tests
go
create table sqlserver_test_table(id int, name text);
go
insert into sqlserver_test_table values (1, 'aaaa');
go