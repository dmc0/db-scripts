/* MIT License

Copyright (c) 2020 David McNamara

DESCRIPTION: This script will search every column in a database that matches one of these data types: 
BINARY, GUID, NTEXT, SQL_VARIANT, UNIQUEIDENTIFIER, VARBINARY, CHAR, NCHAR, NVARCHAR, SYSNAME, TEXT, VARCHAR.  

It will also exclude tables based ON row size between the @rows_min_cnt and @rows_max_cnt variables.  
This is useful to prevent taxing the server by querying against large tables... unless it is
necessary. 
*/

--Enter the value you're looking for.  It can be for a partial, starts with, or exact match.
DECLARE @value_sought VARCHAR(1000) = 'indicator'  

--@match_type options: 1 = contains, 2 = starts with, 3 = exact
DECLARE @match_type tinyint = 1  

/* Normally @rows_min_cnt will be 1 because you normally will want to search tables that have at least 1 row of data.  
There may be rare times you only want to search tables with a minimum number of rows that's greater than 1.
*/
DECLARE @rows_min_cnt int = 1

/* @rows_max_cnt is critical because normally you will not want to search tables with millions of rows or more.  
Usually you would be searching small tables so a value of 100,000 rows will normally be good enough.
*/
DECLARE @rows_max_cnt int = 100000

-- SET @search_system_tables_only to 1 only if you want to search tables built into SQL Server
DECLARE @search_system_tables_only BIT = 0

DECLARE @table_name VARCHAR(128)
DECLARE @column_name VARCHAR(128)
DECLARE @schema_name VARCHAR(128)
DECLARE @row_count int
DECLARE @sql as nvarchar(2000)
DECLARE @row_pos INT
DECLARE @table_column_count INT
DECLARE @result INT
DECLARE @msg NVARCHAR(2000)

SET @row_pos = 0
SET @table_column_count = 0
SET @result = 0

IF OBJECT_ID('tempdb..#table_columns') IS NOT NULL
	DROP TABLE #table_columns

CREATE TABLE #table_columns(RowID INT IDENTITY(1, 1)
, SchemaName SYSNAME
, TableName SYSNAME
, ColumnName SYSNAME
, [RowCount] INT)

BEGIN TRY
	IF @search_system_tables_only = 0
		INSERT #table_columns(SchemaName, TableName, ColumnName, [RowCount])
		SELECT DISTINCT ss.name AS SchemaName
		, so.name AS TableName
		, sc.name AS ColumnName
		, RowCnts.rowcnt AS [RowCount]
		FROM sys.sysobjects so
		INNER JOIN sys.syscolumns sc
		ON so.id = sc.id
		INNER JOIN (SELECT * 
		            FROM sys.systypes 
		            WHERE name IN ('BINARY', 'GUID', 'NTEXT', 'SQL_VARIANT', 'UNIQUEIDENTIFIER', 'VARBINARY', 'CHAR', 'NCHAR', 'NVARCHAR', 'SYSNAME', 'TEXT', 'VARCHAR')
		) st
		ON sc.xusertype = st.xusertype
		INNER JOIN (select o.id, i.rowcnt
			from sysobjects o 
			INNER JOIN sysindexes i 
			ON o.id = i.id 
			WHERE i.indid IN (0, 1) 			
			AND i.rowcnt Between @rows_min_cnt AND @rows_max_cnt) RowCnts
		ON so.id = RowCnts.id
		INNER JOIN [sys].[schemas] ss
		ON so.uid = ss.schema_id
		WHERE so.xtype IN ('U', 'V') 
		ORDER BY so.name

	IF @search_system_tables_only = 1
		INSERT #table_columns(SchemaName, TableName, ColumnName, [RowCount])
		SELECT DISTINCT ss.name AS SchemaName
		, so.name AS TableName
		, sc.name AS ColumnName
		, 1 AS [RowCount]
		FROM sys.system_objects so
		INNER JOIN sys.system_columns sc
		ON so.[object_id] = sc.[object_id]
		INNER JOIN (SELECT * 
		            FROM sys.systypes 
		            WHERE name IN ('BINARY', 'GUID', 'NTEXT', 'SQL_VARIANT', 'UNIQUEIDENTIFIER', 'VARBINARY', 'CHAR', 'NCHAR', 'NVARCHAR', 'SYSNAME', 'TEXT', 'VARCHAR'
		)) st
		ON sc.system_type_id = st.xusertype
		INNER JOIN [sys].[schemas] ss
		ON so.[schema_id] = ss.schema_id
		WHERE so.type IN ('V')

	SELECT @table_column_count = COUNT(*)
	FROM #table_columns
		
	WHILE @row_pos < @table_column_count
		BEGIN TRY
			BEGIN		
				SET @row_pos = @row_pos + 1

				SELECT @schema_name = SchemaName, @table_name = TableName, @column_name = ColumnName, @row_count = [RowCount]
				FROM #table_columns
				WHERE RowID = @row_pos

				SET @sql = 'IF EXISTS(SELECT [' + @column_name + '] FROM ' + @schema_name + '.[' + @table_name + '] 
									  WHERE CAST([' + @column_name + '] AS VARCHAR(MAX))' + (CASE @match_type 
					WHEN 1 THEN ' Like ''%' + @value_sought + '%'
					WHEN 2 THEN ' Like ''' + @value_sought + '%'
					WHEN 3 THEN ' = ''' + @value_sought END) + ''')
						SELECT ''' + @table_name + ''' AS [Table Name]
						, ''' + @column_name + ''' AS [Column Name]
						, * 
						FROM ' + @schema_name + '.[' + @table_name + '] 
						WHERE CAST([' + @column_name + '] AS VARCHAR(MAX))' 					
						+ (CASE @match_type   --1 = contains, 2 = starts with, 3 = exact
						WHEN 1 THEN ' Like ''%' + @value_sought + '%'
						WHEN 2 THEN ' Like ''' + @value_sought + '%'
						WHEN 3 THEN ' = ''' + @value_sought END) + ''''

				PRINT @table_name + '.' + @column_name + ', Row Count = ' + CAST(@row_count AS VARCHAR(20))

				IF @table_name IN ('all_columns', 'system_columns')
					BEGIN
						PRINT 'WARNING: ' + @table_name + ' was skipped to avoid a fatal error.'
					END
				ELSE
					BEGIN
						exec @result = sp_executesql @sql
			
						IF @result <> 0
							BEGIN
								PRINT @sql 
								RAISERROR('Error in select statement', 16, 0)
							END
					END
			END
			END TRY
			BEGIN CATCH
				SET @msg = 'Error Number ' + CAST(ERROR_NUMBER() AS NVARCHAR(24)) + ', Severity ' 
				+ CAST(ERROR_SEVERITY() AS NVARCHAR(24)) + ': ' 
				+ ERROR_MESSAGE()
				PRINT @msg
				CONTINUE
			END CATCH		
END TRY
BEGIN CATCH
	PRINT ISNULL(@sql, '') ;

	THROW;
END CATCH
GO
