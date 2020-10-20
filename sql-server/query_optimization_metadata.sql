/* MIT License

Copyright (c) 2020 David McNamara

DESCRIPTION: The script is designed to help make performance tuning decisions for 
queries that use many tables, joins, and where conditions.  
Add the table names you're working on to the VALUES for the #tables table.

*/


SET NOCOUNT ON

BEGIN TRY
	IF OBJECT_ID('tempdb..#tables') IS NOT NULL DROP TABLE #tables
	CREATE TABLE #tables (row_id int IDENTITY(1, 1), sort_order int, table_name NVARCHAR(128) PRIMARY KEY)
	
	INSERT #tables(table_name)
	VALUES ('t_song')
	, ('t_song_publisher')
	, ('t_song_stream_play')
	, ('')

	DECLARE @show_table_column_metadata BIT = 1
	DECLARE @show_row_counts_table BIT = 1
	DECLARE @show_primary_and_foreign_keys_table BIT = 1
	DECLARE @show_primary_keys_table BIT = 1

	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

	--add all tables if there are none in #tables
	IF NOT EXISTS (SELECT 1 FROM #tables WHERE LEN(ISNULL([table_name], '')) > 0)
		BEGIN
			INSERT #tables(table_name)
			SELECT st.[name]
			FROM sys.tables st 
		END

	--Make sort_order alphabetical
	UPDATE #tables
	SET sort_order = ts.sort_order
	FROM #tables tn
	INNER JOIN (SELECT table_name
		, sort_order = ROW_NUMBER() OVER (ORDER BY table_name)
		FROM #tables) ts
	on tn.table_name = ts.table_name
		
	IF OBJECT_ID('tempdb..#pk_fk') IS NOT NULL
		DROP TABLE #pk_fk

	IF OBJECT_ID('tempdb..#row_counts') IS NOT NULL
		DROP TABLE #row_counts

	SELECT [schema_name] = s.[name]
	, [table_name] = t.[name] 
	, [row_count] = SUM(st.row_count)
	INTO #row_counts
	FROM sys.tables t 
	INNER JOIN #tables tn
	on t.[name] = tn.table_name
	INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
	INNER JOIN sys.indexes i ON t.object_id = i.object_id
	INNER JOIN sys.dm_db_partition_stats st ON t.object_id = st.object_id and i.index_id = st.index_id
	WHERE i.index_id < 2
	GROUP BY s.[name], t.[name]

	--get foreign key data
	;with cte AS (SELECT pk_name = si.[name]
		, primary_schema = ps.[name]
		, primary_table = pt.[name]
		, primary_col = sc.[name] 
		, f.[name] AS fk_name
		, foreign_table = ft.[name] 
		, foreign_col = fc.[name]
		, foreign_schema = fs.[name] 
		FROM sys.foreign_keys f 
		INNER JOIN sys.schemas fs on f.[SCHEMA_ID] = fs.[SCHEMA_ID]
		INNER JOIN sys.foreign_key_columns fkc ON f.[object_id] = fkc.constraint_object_id 
		INNER JOIN sys.columns fc ON fkc.parent_object_id = fc.[object_id] and fkc.parent_column_id = fc.column_id
		INNER JOIN sys.objects pt ON fkc.referenced_object_id = pt.[object_id] 
		INNER JOIN sys.schemas ps on pt.[SCHEMA_ID] = ps.[SCHEMA_ID]
		INNER JOIN sys.objects ft on f.parent_object_id = ft.[object_id]
		INNER JOIN sys.columns sc ON fkc.referenced_object_id = sc.[object_id] and fkc.referenced_column_id = sc.column_id
		INNER JOIN (SELECT * FROM [sys].[indexes] WHERE [is_primary_key] = 1) si
		ON pt.[object_id] = si.[object_id]
	)
	, cte2 AS (SELECT CAST((ROW_NUMBER() OVER(ORDER BY primary_schema, primary_table, primary_col, foreign_table, foreign_col) + 4) AS NVARCHAR(24)) AS [row_id]	
		, *
		FROM cte
		WHERE primary_table IN (SELECT table_name FROM #tables)
	OR foreign_table IN (SELECT table_name FROM #tables))

	SELECT *
	INTO #pk_fk
	FROM cte2

	--get primary and foreign key data, clustered and non-clustered indexes, and row counts
	SELECT index_type = (CASE si.[type] WHEN 1 THEN 'Clustered' WHEN 2 THEN 'Non-clustered' ELSE 'Unknown' END)
	, table_name = st.[name]
	, [row_count_fmt] = FORMAT(rc.[row_count], '#,##0', 'en-US')
	, index_name = si.[name] 
	, incl_col = ic.is_included_column
	, col_ord = ic.key_ordinal
	, [unique] = si.is_unique
	, col_name = sc.[name]
	, k.foreign_table
	, k.foreign_col
	, ic.is_descending_key
	FROM sys.tables st 
	INNER JOIN #tables tn
	ON st.[name] = tn.table_name
	INNER JOIN #row_counts rc
	ON tn.table_name = rc.table_name 
	INNER JOIN [sys].[indexes] si 
	ON st.[object_id] = si.[object_id]
	INNER JOIN sys.syscolumns sc 
	ON sc.id = si.[object_id]
	INNER JOIN sys.index_columns ic 
	ON st.[object_id] = ic.[object_id] AND ic.column_id = sc.colid AND si.index_id = ic.[index_id]
	left join (SELECT * 
		FROM #pk_fk k 
		WHERE primary_table IN (SELECT table_name FROM #tables)
		AND foreign_table IN (SELECT table_name FROM #tables)) k
	on st.[name] = k.primary_table and si.[name] = k.pk_name 
	WHERE si.[type] IN (1, 2)
	ORDER BY tn.sort_order, table_name, index_type DESC, index_name, ic.is_included_column, ic.key_ordinal

	IF @show_table_column_metadata = 1		
		SELECT kcu.[TABLE_CATALOG]
			  , kcu.[TABLE_SCHEMA]
			  , kcu.[TABLE_NAME]
			  , kcu.[COLUMN_NAME]
			  , kcu.[ORDINAL_POSITION]
			  , c.DATA_TYPE
			  , tc.CONSTRAINT_TYPE
			  , is_identity = ISNULL(ic.is_identity, 0)
			  , c.*
		  FROM [INFORMATION_SCHEMA].[KEY_COLUMN_USAGE] kcu 
		  INNER JOIN [INFORMATION_SCHEMA].[COLUMNS] c 
		  on kcu.TABLE_CATALOG = c.TABLE_CATALOG
		  AND kcu.TABLE_SCHEMA = c.TABLE_SCHEMA
		  AND kcu.TABLE_NAME = c.TABLE_NAME
		  AND kcu.COLUMN_NAME = c.COLUMN_NAME
		  INNER JOIN [INFORMATION_SCHEMA].[TABLE_CONSTRAINTS] tc 
		  on kcu.CONSTRAINT_CATALOG = tc.CONSTRAINT_CATALOG
		  AND kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
		  AND kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
		  LEFT join (select TABLE_NAME = OBJECT_NAME(object_id), COLUMN_NAME = [name], is_identity 
					from sys.columns where is_identity = 1) ic
			on c.TABLE_NAME = ic.TABLE_NAME
			AND c.COLUMN_NAME = ic.COLUMN_NAME
		  where tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
		  AND kcu.TABLE_NAME IN (SELECT table_name FROM #tables)

	IF @show_row_counts_table = 1
		SELECT rc.table_name
		, [row_count] = FORMAT(rc.[row_count], '#,##0', 'en-US')
		, tn.sort_order
		FROM #row_counts rc
		INNER JOIN #tables tn
		on rc.table_name = tn.table_name	
		ORDER BY tn.sort_order	
	
	IF @show_primary_and_foreign_keys_table = 1
		SELECT *
		FROM #pk_fk
		ORDER BY primary_table, primary_col, foreign_table, foreign_col, fk_name

	IF @show_primary_keys_table = 1
		--table name, column names, primary key index name
		SELECT st.name AS table_name
		, si.name AS pk_name
		, sc.name AS [col_name]
		FROM  (SELECT * FROM [sys].[indexes] WHERE [is_primary_key] = 1) si
		INNER JOIN sys.index_columns ic 
		ON si.[object_id] = ic.[object_id]  AND si.index_id = ic.index_id 
		INNER JOIN sys.syscolumns sc 
		ON ic.column_id = sc.colid AND si.[object_id] = sc.id
		INNER JOIN sys.tables st 
		ON st.[object_id] = si.[object_id]
		WHERE st.name IN (SELECT table_name FROM #tables)
		ORDER BY table_name, pk_name, ic.key_ordinal
END TRY
BEGIN CATCH
	PRINT 'perf_metadata_for_query_optimization failed';
	THROW;
END CATCH
