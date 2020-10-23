/* MIT License

Copyright (c) 2020 David McNamara

DESCRIPTION: This script is meant to be used in a SQL Server Agent job.  
	--The @file_number parameter defaults to the day of the month.  
	This means that a backup file will be kept around for about a month because it is replaced 
	when the same day of the month occurs next.  
	--If you want backup files to be kept around longer than a month, you might use a value 
	like DATEPART(DAYOFYEAR, GETDATE()) to keep backups around for a year.  
	--If you want backup files to be kept around less than a month, you might use a value 
	like DATEPART(WEEKDAY, GETDATE()) to keep backups around for a week. 

REQUIREMENTS: You need to enter the names of the databases you want to back up in the "WHERE [name] IN ('')" clause.  
You also need to pass in the full path where you want the backup files to go, 
or set its default parameter value.
*/

ALTER  --CREATE
PROC p_backup_databases
-- specify database backup directory
@backup_path VARCHAR(256) = 'D:\D_Temp\'  
AS

DECLARE db_cursor CURSOR LOCAL FORWARD_ONLY FAST_FORWARD READ_ONLY FOR
SELECT [name] 
FROM [master].dbo.sysdatabases 
WHERE [name] IN ('DWConfiguration', 'DWDiagnostics')  

DECLARE @server_instance VARCHAR(256) = REPLACE(CAST(SERVERPROPERTY('ServerName') AS VARCHAR), '\', '_')
DECLARE @file_number INT = DATEPART(dw, GETDATE())   --DAY(GETDATE()) 

IF @file_number = 6  --on Friday do weekly backup
	SET @file_number = (100 + DATEPART(wk, GETDATE()))

BEGIN TRY
	DECLARE @debug BIT = 1
	DECLARE @db_name VARCHAR(128)  
	DECLARE @backup_file_name VARCHAR(256) -- filename for backup 

	OPEN db_cursor 
	FETCH NEXT FROM db_cursor INTO @db_name   

	IF @@FETCH_STATUS <> 0
		RAISERROR('No database names matching those in the IN clause were found on this server', 16, 1)

	WHILE @@FETCH_STATUS = 0
	BEGIN
		--These statements must be run immediately one after the other while no other data manipulation 
		-- operations are taking place on the database		
	
		DECLARE @temp_log_file_name NVARCHAR(1024) = ''

		SET @db_name = REPLACE(REPLACE(@db_name, '[', ''), ']', '')

		PRINT 'Processing ' + @db_name + '...'

		DECLARE @sql NVARCHAR(MAX)
	
		SET @sql = '--can only backup log if database supports FULL or BULK-LOGGED recovery models
			IF EXISTS (SELECT database_id 
			FROM [master].sys.databases
			WHERE recovery_model IN (1,2)
			AND [name] = ''' + @db_name + ''')
				IF EXISTS ( SELECT database_name
					FROM [msdb].[dbo].[backupset]
					WHERE database_name = ''' + @db_name + ''')
				BEGIN
					BACKUP LOG [' + @db_name + '] TO DISK = ''' + @db_name + '_log.bak''
				END'

		IF @debug = 1 PRINT @sql

		EXEC sys.sp_executesql @sql

		SELECT @temp_log_file_name = [name]
		FROM [master].[sys].[master_files]
		WHERE [TYPE] = 1 AND [database_id] = DB_ID(N'' + @db_name + '')

		SET @sql = 'USE [' + @db_name + ']
			DBCC SHRINKFILE (''' + @temp_log_file_name + ''' , 0, TRUNCATEONLY)'

		IF @debug = 1 PRINT @sql

		EXEC sys.sp_executesql @sql

		IF @debug = 1 
			BEGIN
				SET @sql = 'sp_helpdb [' + @db_name + ']'

				PRINT @sql

				EXEC sys.sp_executesql @sql	
			END
  
		SET @backup_file_name = @backup_path + @server_instance + '_' + @db_name + '_' + CAST(@file_number AS VARCHAR(20)) + '.BAK'
	    
		DECLARE @backup_name NVARCHAR(1024) = @db_name + N'-Full Database Backup'

		BACKUP DATABASE @db_name TO DISK = @backup_file_name WITH NOFORMAT, INIT, name = @backup_name, SKIP, NOREWIND
		, NOUNLOAD,  STATS = 10

		FETCH NEXT FROM db_cursor INTO @db_name   
	END

	CLOSE db_cursor   
	DEALLOCATE db_cursor
END TRY
BEGIN CATCH
	CLOSE db_cursor;  
	DEALLOCATE db_cursor;

	THROW;
END CATCH