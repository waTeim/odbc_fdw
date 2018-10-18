-- This fixture is going to be executed with TSQL

-- Drop everything, dropping the database is not an option in Azure
DECLARE @sql NVARCHAR(max)=''
begin
	SELECT @sql += ' ALTER TABLE ' + QUOTENAME(TABLE_SCHEMA) + '.'+ QUOTENAME(TABLE_NAME) +    ' NOCHECK CONSTRAINT all; '
	FROM   INFORMATION_SCHEMA.TABLES
	WHERE TABLE_TYPE = 'BASE TABLE'

	Exec Sp_executesql @sql
end

begin
	select @sql = ''

	SELECT @sql += ' Drop table ' + QUOTENAME(TABLE_SCHEMA) + '.'+ QUOTENAME(TABLE_NAME) + '; '
	FROM   INFORMATION_SCHEMA.TABLES
	WHERE TABLE_TYPE = 'BASE TABLE'
	Exec Sp_executesql @sql
end


-- Create fixture
create table sqlserver_test_table(id int, name text);
go
insert into sqlserver_test_table values (1, 'aaaa');
go
