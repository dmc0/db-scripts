/* MIT License

Copyright (c) 2020 David McNamara

DESCRIPTION: This script should help you move a live database from one location to another without performing
a backup and restore.  It may be especially useful for large databases because it may be quicker to use this
method vs. backup and restore.

It generates a 5-step process with instructions and SQL statements for each step.

REQUIREMENTS:
1. Enter the values for the @db_name and @new_db_file_path variables below and run the script.
2. Follow the 5 steps that are printed to the Messages tab window.

*/

DECLARE @db_name VARCHAR(128) = 'dn_name_goes_here'
DECLARE @new_db_file_path NVARCHAR(1024) = 'C:\Temp\'


USE master

BEGIN TRY
	SET @db_name = TRIM(@db_name)

	DECLARE @sql NVARCHAR(MAX) = ''
	DECLARE @sql_step NVARCHAR(MAX) = ''
	DECLARE @result INT = 0
	DECLARE @current_db_path NVARCHAR(1024)

	IF RIGHT(@new_db_file_path, 1) <> '\'
		SET @new_db_file_path = @new_db_file_path + '\'

	SET @sql = 'SELECT @current_db_path = [filename]
	FROM ' + QUOTENAME(@db_name) + '.sys.sysfiles
	WHERE fileid = 1'

	exec @result = sp_executesql @sql, N'@current_db_path NVARCHAR(1024) OUTPUT', @current_db_path = @current_db_path OUTPUT

	IF @result <> 0
	BEGIN
		RAISERROR('Setting @current_db_path failed', 16, 1)
		RETURN
	END

	SET @current_db_path = SUBSTRING(@current_db_path, 1, (LEN(@current_db_path) - CHARINDEX('\', REVERSE(@current_db_path), 1) + 1))

	SET @sql_step = '-- Step 1. Select the commands below and execute them in SSMS... it could take a minute or two to complete

	USE master
	ALTER DATABASE ' + QUOTENAME(@db_name) + ' SET OFFLINE WITH ROLLBACK IMMEDIATE;
	RETURN


	'

	PRINT @sql_step

	SET @sql_step = '/*
	Step 2. Go to File Explorer and MOVE the database files from

	' + @current_db_path + '
	to
	' + @new_db_file_path + '

	!!! Make sure the permissions are the same on each folder... or the SQL Server service might not be able to access the files after the move !!!
	*/


	'

	PRINT @sql_step

	SET @sql_step = ''

	SET @sql = 'SELECT @sql_step = @sql_step + ''ALTER DATABASE ' + QUOTENAME(@db_name) + ' MODIFY FILE ( NAME = '' + [name] + '', FILENAME = ''''' + @new_db_file_path + ''' + SUBSTRING([filename], (LEN([filename]) - CHARINDEX(''\'', REVERSE([filename]), 1) + 2), LEN([filename])) + '''''');'' + CHAR(13) + CHAR(10)
	FROM ' + QUOTENAME(@db_name) + '.sys.sysfiles'

	exec @result = sp_executesql @sql, N'@sql_step NVARCHAR(MAX) OUTPUT', @sql_step = @sql_step OUTPUT

	IF @result <> 0
	BEGIN
		RAISERROR('ALTER DATABASE MODIFY FILE failed', 16, 1)
		RETURN
	END

	SET @sql_step = '-- Step 3. Select the commands below and execute them in SSMS

	USE master
	' + @sql_step + '
	RETURN


	'

	PRINT @sql_step

	SET @sql_step = '
	-- Step 4. Select the commands below and execute them in SSMS

	USE master
	ALTER DATABASE ' + QUOTENAME(@db_name) + ' SET ONLINE;
	RETURN


	'

	PRINT @sql_step

	SET @sql_step = '
	-- Step 5. Select the commands below and execute them in SSMS to verify the database files are correct

	USE master
	SELECT name, physical_name AS CurrentLocation, state_desc
	FROM sys.master_files
	WHERE database_id = DB_ID(N''' + @db_name + ''');

	RETURN

	'

	PRINT @sql_step
END TRY
BEGIN CATCH
	PRINT 'database_generate_move_script failed';
	THROW;
END CATCH