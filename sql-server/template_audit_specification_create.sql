/* MIT License

Copyright (c) 2020 David McNamara

DESCRIPTION: The script will start auditing immediately because (STATE = ON).  
Also, for more information see http://solutioncenter.apexsql.com/sql-server-database-auditing-techniques/
*/

DECLARE @audit_name NVARCHAR(128) = 'audit_App'
DECLARE @audit_spec_name NVARCHAR(128) = 'audit_app_config'

DECLARE @db_name NVARCHAR(128) = 'DWConfiguration'   --DWConfiguration
DECLARE @table_name NVARCHAR(128) = 'node'  -- table to audit
DECLARE @drop_previous_audits BIT = 1  /* 1 = YES, 0 = NO */

BEGIN TRY
	DECLARE @sql NVARCHAR(4000)
	DECLARE @file_path NVARCHAR(1024) = CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS NVARCHAR(1024))
	PRINT 'The audit file is will be in this folder: ' + @file_path

	SET @sql = 
		CASE @drop_previous_audits 
			WHEN 1 THEN 'IF EXISTS (SELECT 1 FROM sys.server_audits WHERE [name] = ''' + @audit_name + ''')
			BEGIN
				ALTER SERVER AUDIT [' + @audit_name + '] WITH (STATE = OFF)
				DROP SERVER AUDIT [' + @audit_name + ']
			END'
			ELSE ''
		END 
	
	+ '
	CREATE SERVER AUDIT [' + @audit_name + ']
	TO FILE 
	(      FILEPATH = N''' + @file_path + '''
		   , MAXSIZE = 0 MB
		   , MAX_ROLLOVER_FILES = 2147483647
		   , RESERVE_DISK_SPACE = OFF
	)
	WITH
	(      QUEUE_DELAY = 1000
		   , ON_FAILURE = CONTINUE
	)
	ALTER SERVER AUDIT [' + @audit_name + '] WITH (STATE = ON)
	'

	exec master.dbo.sp_executesql @sql

	SET @sql = 'USE [' + @db_name + ']'

	SET @sql += 
		CASE @drop_previous_audits 
			WHEN 1 THEN '
	IF EXISTS (SELECT 1 FROM sys.database_audit_specifications WHERE [name] = ''' + @audit_spec_name + ''')
		BEGIN
			ALTER DATABASE AUDIT SPECIFICATION [' + @audit_spec_name + '] WITH (STATE = OFF)
			DROP DATABASE AUDIT SPECIFICATION [' + @audit_spec_name + ']
		END'
			ELSE ''
		END 
		+ '
	CREATE DATABASE AUDIT SPECIFICATION [' + @audit_spec_name + ']
	FOR SERVER AUDIT [' + @audit_name + ']

	ADD (INSERT ON OBJECT::[dbo].[' + @table_name + '] BY [dbo]),
	ADD (UPDATE ON OBJECT::[dbo].[' + @table_name + '] BY [dbo]),
	ADD (DELETE ON OBJECT::[dbo].[' + @table_name + '] BY [dbo])
	WITH (STATE = ON)'	

	exec dbo.sp_executesql @sql
END TRY
BEGIN CATCH
	THROW;
END CATCH