/*-------------------------------------------------------------------------
 *
 *                foreign-data wrapper for ODBC
 *
 * Copyright (c) 2011, PostgreSQL Global Development Group
 * Copyright (c) 2016, CARTO
 *
 * This software is released under the PostgreSQL Licence
 *
 * Original author: Zheng Yang <zhengyang4k@gmail.com>
 *
 * IDENTIFICATION
 *                odbc_fdw/odbc_fdw--0.2.0.sql
 *
 *-------------------------------------------------------------------------
 */

CREATE OR REPLACE FUNCTION odbc_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION odbc_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

ALTER FOREIGN DATA WRAPPER odbc_fdw
  HANDLER odbc_fdw_handler
  VALIDATOR odbc_fdw_validator;

DROP FUNCTION IF EXISTS ODBCTablesList(text, integer);
DROP FUNCTION IF EXISTS ODBCTableSize(text, text);
DROP FUNCTION IF EXISTS ODBCQuerySize(text, text);

DROP TYPE __tabledata;
