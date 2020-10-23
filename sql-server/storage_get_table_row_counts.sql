/* MIT License

Copyright (c) 2020 David McNamara

DESCRIPTION: This script is based on the disk and table reports available in SSMS for 
a SQL Server database.  I captured the SQL Profiler activity while running those reports
and modified them to suit most peoples' needs.  I also modified the code so that it will
not fail no matter how many tables are locked up and not allowing row counts.  
This means that this code will succeed even in cases where the SSMS reports will fail due to
extreme and prolonged locking.

PURPOSE: The motivation for the script was to obtain the row counts of as many tables as
possible even if some are locked and their rows cannot be counted. 
If it fails to get the row_count for a table, it will display an error message and the 
get_count_failed value will remain set to 1 and it's row_count will be -1. 
If an error like this occurs, the results will show the get_count_failed tables at the top
to make you more aware that some row counts could not be determined. 

REQUIREMENT: You need to enter the database names you want the row counts for in the
VALUES list for INSERT #database_names([database_name])
*/

IF OBJECT_ID('tempdb..#database_names') IS NOT NULL DROP TABLE #database_names
CREATE TABLE #database_names([database_name] NVARCHAR(128))

-- !!!!! Enter the names of the databases you want to get the row counts for  !!!!!
INSERT #database_names([database_name])
VALUES ('DWConfiguration')  --DWConfiguration
, ('DWDiagnostics')  --DWDiagnostics
, ('')

-- This is in milliseconds.  IF you need to increase it, you need to break the connection 
-- before a new value will take effect.
SET LOCK_TIMEOUT 1800;  
DECLARE @debug BIT = 0

SET NOCOUNT ON
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

BEGIN TRY
	IF OBJECT_ID('tempdb..#table_row_counts') IS NOT NULL
		DROP TABLE #table_row_counts

	IF OBJECT_ID('tempdb..#table_names') IS NOT NULL
		DROP TABLE #table_names

	DECLARE @table_count INT = 0
	DECLARE @table_pos INT = 0
	DECLARE @table_name NVARCHAR(128)
	DECLARE @object_id INT = 0
	DECLARE @table_row_count INT = 0

	DECLARE @database_count INT = 0
	DECLARE @database_pos INT = 0
	DECLARE @database_name NVARCHAR(128)
	DECLARE @schema_name NVARCHAR(128)
	DECLARE @table_create_date datetime

	DECLARE @user_msg NVARCHAR(2000)
	DECLARE @sql NVARCHAR(4000)

	CREATE TABLE #table_row_counts(row_id INT identity(1, 1)
	, [database_name] NVARCHAR(128)
	, [schema_name] nvarchar(128)
	, [table_name] nvarchar(128)
	, row_count BIGINT DEFAULT(-1)
	, table_create_date datetime
	, get_count_failed BIT DEFAULT(1))

	CREATE TABLE #table_names(row_id INT
	, [database_name] NVARCHAR(128)
	, [schema_name] NVARCHAR(128)
	, [table_name] nvarchar(128)
	, [object_id] INT
	, table_create_date datetime)

	SELECT @database_count = COUNT(DISTINCT [database_name])
	FROM #database_names
	WHERE LEN(ISNULL([database_name], '')) > 0

	SET @user_msg = '@database_count = ' + CAST(@database_count AS NVARCHAR(24))
	RAISERROR(@user_msg, 10, 1) WITH NOWAIT

	UPDATE #database_names
	SET [database_name] = REPLACE(REPLACE([database_name], '[', ''), ']', '')
	WHERE [database_name] Like '%![%' ESCAPE '!' OR [database_name] Like '%!]%' ESCAPE '!'

	SET @database_pos = 0

	WHILE @database_pos < @database_count
		BEGIN		
			SET @database_pos += 1

			;WITH cte as (SELECT DISTINCT [database_name]
				FROM #database_names
				WHERE LEN(ISNULL([database_name], '')) > 0)

			SELECT @database_name = [database_name]
			FROM (SELECT [database_name], row_id = RANK() OVER (ORDER BY [database_name])
				FROM cte) d
			WHERE d.row_id = @database_pos

			IF db_id(@database_name) is null
				BEGIN
					SET @user_msg = '@database_name = ' + @database_name + ' does not exist'
					RAISERROR(@user_msg, 16, 1) WITH NOWAIT
				END

			SET @user_msg = '@database_name = ' + @database_name
			RAISERROR(@user_msg, 10, 1) WITH NOWAIT

			TRUNCATE TABLE #table_names

			SET @sql = 'INSERT #table_names(row_id, [database_name], [schema_name], [table_name], [object_id], [table_create_date])
			SELECT row_id = ROW_NUMBER() OVER (ORDER BY s.name, t.[name]), ''' + @database_name + ''', [schema_name] = s.name, t.[name], t.[object_id], t.create_date
			FROM [' + @database_name + '].sys.tables t
			INNER JOIN [' + @database_name + '].sys.schemas s
			ON t.schema_id = s.schema_id'

			EXEC sp_executesql @sql

			SET @table_count = @@ROWCOUNT

			SET @table_pos = 0

			WHILE @table_pos < @table_count
				BEGIN	
					BEGIN TRY			
						SET @table_pos += 1

						SELECT @schema_name = [schema_name]
						, @table_name = [table_name]
						, @object_id = [object_id]
						, @table_create_date = [table_create_date]
						FROM #table_names
						WHERE row_id = @table_pos

						SET @user_msg = '@table_name = ' + @table_name 
						RAISERROR(@user_msg, 10, 1) WITH NOWAIT

						INSERT #table_row_counts([database_name], [schema_name], [table_name], [table_create_date])
						VALUES(@database_name, @schema_name, @table_name, @table_create_date)

						SET @sql = 'SELECT @table_row_count = SUM(st.row_count)
						FROM (SELECT [object_id], index_id 
							FROM [' + @database_name + '].sys.indexes i
							WHERE [object_id] = @object_id) i 
						INNER JOIN [' + @database_name + '].sys.dm_db_partition_stats st WITH (READCOMMITTED, READPAST) 
						ON st.[object_id] = @object_id and i.index_id = st.index_id
						WHERE i.index_id < 2'

						IF @debug = 1
							BEGIN
								SET @user_msg = '@sql = ' + @sql 
								RAISERROR(@user_msg, 10, 1) WITH NOWAIT
							END

						EXEC sp_executesql @sql, N'@object_id INT, @table_row_count INT OUTPUT', @object_id = @object_id, @table_row_count = @table_row_count OUTPUT

						UPDATE #table_row_counts
						SET row_count = @table_row_count
						, get_count_failed = 0
						WHERE [database_name] = @database_name
						AND [schema_name] = @schema_name
						AND table_name = @table_name

						SET @user_msg = '@table_name = ' + @table_name + ', @table_row_count = ' + CAST(@table_row_count AS NVARCHAR(24))
						RAISERROR(@user_msg, 10, 1) WITH NOWAIT

					END TRY
					BEGIN CATCH
						SET @user_msg = '@table_name = ' + ISNULL(@table_name, '') + ': ' + ERROR_MESSAGE()
						SELECT [!!!  ERROR  !!!] = @user_msg
						RAISERROR(@user_msg, 16, 1) WITH NOWAIT
					END CATCH
				END
		END


	SELECT 'Sorted by row count' AS [report_name],  [database_name], [schema_name], table_name, [row_count_disp] = FORMAT([row_count], '#,##0'), [get_count_failed], [table_create_date]
	FROM #table_row_counts
	order by [get_count_failed] desc, [row_count] desc, [database_name], table_name

	SELECT 'Sorted by database, table' AS [report_name], [database_name], [schema_name], table_name, [row_count_disp] = FORMAT([row_count], '#,##0'), [get_count_failed], [table_create_date]
	FROM #table_row_counts
	order by [get_count_failed] desc, [database_name], table_name
END TRY
BEGIN CATCH
	THROW;
END CATCH
