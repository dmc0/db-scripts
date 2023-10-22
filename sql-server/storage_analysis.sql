/* MIT License

Copyright (c) 2020 David McNamara

DESCRIPTION: The core of this script is based ON the reports available in SSMS.
SQL Profiler was used to reverse engineer the queries used by those reports
and that code was modified according to practical needs.

This will give you multiple views of the storage used by tables and indexes.  
It does totals by drive, folder, file, table, index, and filegroup. 
In the future WITH SQL Server 2012 and higher start WITH dm_db_database_page_allocations 
to determine how much space each file is using when the data is spread across multiple files.
*/

DECLARE @db_list TABLE(row_id INT IDENTITY(1, 1), [db_name] NVARCHAR(128))

INSERT @db_list([db_name])
VALUES ('DWConfiguration')  --DWConfiguration
, ('DWDiagnostics')  --DWDiagnostics
, ('tempdb')

DECLARE @debug bit = 0

SET NOCOUNT ON

BEGIN TRY
	IF OBJECT_ID('tempdb..#storage_metrics') IS NOT NULL
		DROP TABLE #storage_metrics

	CREATE TABLE #storage_metrics([db_name] NVARCHAR(128)
	, [schema_name] NVARCHAR(128)
	, [table_name] NVARCHAR(128)
	, [object_id] INT
	, table_created_date datetime2
	, [row_count] BIGINT)

	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

	DECLARE @sql NVARCHAR(MAX) = ''
	DECLARE @row_pos INT = 0
	DECLARE @row_count INT = (SELECT COUNT(*) FROM @db_list)
	DECLARE @db_name NVARCHAR(128)
	DECLARE @result Int = 0

	WHILE @row_pos < @row_count
		BEGIN
			SET @row_pos +=1

			SELECT @db_name = [db_name]
			FROM @db_list
			WHERE row_id = @row_pos
		
			PRINT '@db_name = ' + @db_name

			SET @sql = 'INSERT #storage_metrics
			SELECT [db_name] = ''' + @db_name + ''', [schema_name] = s.name , [table_name] = t.name , t.[object_id], t.create_date, SUM(st.row_count) AS [row_count]
			FROM [' + @db_name + '].sys.tables t 
			INNER JOIN [' + @db_name + '].sys.schemas s   ON t.schema_id = s.schema_id
			INNER JOIN [' + @db_name + '].sys.indexes i   ON t.object_id = i.object_id
			INNER JOIN [' + @db_name + '].sys.dm_db_partition_stats st   ON t.object_id = st.object_id and i.index_id = st.index_id
			WHERE i.index_id < 2
			GROUP BY s.name, t.name, t.[object_id], t.create_date'

			exec @result = sp_executesql @sql

			IF @result <> 0
				BEGIN
					RAISERROR('INSERT #storage_metrics failed', 16, 1)
					RETURN
				END
		END

	IF OBJECT_ID('tempdb..#db_stat_table') IS NOT NULL
		DROP TABLE #db_stat_table

	CREATE TABLE #db_stat_table([db_name] NVARCHAR(128)
	, [schema_name] NVARCHAR(128)
	, [table_name] NVARCHAR(128)
	, [row_count] BIGINT
	, reserved_kb INT
	, data_kb INT
	, index_kb INT
	, unused_kb INT
	, [object_id] INT
	, table_created_date datetime2
	)

	IF OBJECT_ID('tempdb..#db_table_index_files') IS NOT NULL
		DROP TABLE #db_table_index_files

	CREATE TABLE #db_table_index_files([row_id] int IDENTITY(1, 1)
	, [object_type] NVARCHAR(128)
	, [db_name] NVARCHAR(128)
	, [schema_name] NVARCHAR(128)
	, [table_name] NVARCHAR(128)
	, [index_name] NVARCHAR(128)
	, [row_count] BIGINT
	, [reserved_kb] INT
	, [data_used_kb] INT
	, [index_used_kb] INT
	, [unused_kb] INT
	, [file_group_name] NVARCHAR(128)
	, [file_names] NVARCHAR(MAX)
	)

	SET @row_pos = 0

	WHILE @row_pos < @row_count
		BEGIN
			SET @row_pos +=1

			SELECT @db_name = [db_name]
			FROM @db_list
			WHERE row_id = @row_pos

			SET @sql = 'WITH cte_pages as (
				SELECT object_id, SUM (reserved_page_count) as reserved_pages, SUM (used_page_count) as used_pages,
						SUM (case 
								when (index_id < 2) then (in_row_data_page_count + lob_used_page_count + row_overflow_used_page_count)
								else lob_used_page_count + row_overflow_used_page_count
							 end) as cte_pages
				FROM [' + @db_name + '].sys.dm_db_partition_stats 
				GROUP BY object_id
			), extra as (
				SELECT p.object_id, sum(reserved_page_count) as reserved_pages, sum(used_page_count) as used_pages
				FROM [' + @db_name + '].sys.dm_db_partition_stats p  
				INNER JOIN  [' + @db_name + '].sys.internal_tables it  
				ON p.object_id = it.object_id
				WHERE it.internal_type IN (202, 204, 211, 212, 213, 214, 215, 216)
				GROUP BY p.object_id
			)

			INSERT #db_stat_table
			SELECT sm.[db_name]
			, [schema_name] 
			, sm.[table_name]
			, sm.[row_count]
			, (p.reserved_pages + isnull(e.reserved_pages, 0)) * 8 as reserved_kb
			, cte_pages * 8 as data_kb
			, (CASE WHEN p.used_pages + isnull(e.used_pages, 0) > cte_pages THEN (p.used_pages + isnull(e.used_pages, 0) - cte_pages) ELSE 0 END) * 8 as index_kb
			, (CASE WHEN p.reserved_pages + isnull(e.reserved_pages, 0) > p.used_pages + isnull(e.used_pages, 0) THEN (p.reserved_pages + isnull(e.reserved_pages, 0) - p.used_pages + isnull(e.used_pages, 0)) else 0 end) * 8 as unused_kb
			, sm.object_id
			, sm.table_created_date
			from cte_pages p
			INNER JOIN #storage_metrics sm
			ON p.object_id = sm.object_id
			AND sm.[db_name] = ''' + @db_name + '''
			LEFT OUTER JOIN extra e ON p.object_id = e.object_id
	'

			IF @debug = 1 PRINT '@sql = ' + @sql

			exec @result = sp_executesql @sql

			IF @result <> 0
				BEGIN
					RAISERROR('INSERT #db_stat_table failed', 16, 1)
					RETURN
				END	

			SET @sql = '				
			INSERT #db_table_index_files ([object_type]
			, [db_name] 
			, [schema_name] 
			, [table_name] 
			, [index_name]
			, [row_count] 
			, [reserved_kb] 
			, [data_used_kb] 
			, [index_used_kb] 
			, [unused_kb] 
			, [file_group_name] 
			, [file_names])
			SELECT [object_type] = (CASE WHEN ps_1.index_id < 2 THEN ''TABLE'' ELSE ''INDEX'' END)
			, [db_name] = ''' + @db_name + '''
			, [schema_name]  = s_3.name
			, [table_name] = ao_2.name
			, [index_name] = (CASE WHEN ps_1.index_id < 2 THEN '''' ELSE i.[name] END)
			, [row_count]  = ps_1.rows
			, [reserved_kb]  = (ps_1.reserved + ISNULL(it_ps_4.reserved,0)) * 8 
			, [data_used_kb]  = ps_1.data * 8 
			, [index_used_kb] = (CASE WHEN (ps_1.used + ISNULL(it_ps_4.used,0)) > ps_1.data THEN (ps_1.used + ISNULL(it_ps_4.used,0)) - ps_1.data ELSE 0 END) * 8 
			, [unused_kb] = (CASE WHEN (ps_1.reserved + ISNULL(it_ps_4.reserved,0)) > ps_1.used THEN (ps_1.reserved + ISNULL(it_ps_4.reserved,0)) - ps_1.used ELSE 0 END) * 8 
			, [file_group_name] = fg.[name]
			, [file_names] = STUFF((SELECT '', '' + [physical_name] FROM [' + @db_name + '].[sys].[database_files] WHERE [data_space_id] = i.[data_space_id] FOR XML PATH('''')), 1, 2, '''')
			FROM
				(SELECT 
					ps.object_id,
					ps.index_id,
					SUM (
						CASE
							WHEN (ps.index_id < 2) THEN row_count
							ELSE 0
						END
						) AS [rows],
					SUM (ps.reserved_page_count) AS reserved,
					SUM (
						CASE
							WHEN (ps.index_id < 2) THEN (ps.in_row_data_page_count + ps.lob_used_page_count + ps.row_overflow_used_page_count)
							ELSE (ps.lob_used_page_count + ps.row_overflow_used_page_count)
						END
						) AS data,
					SUM (ps.used_page_count) AS used
				FROM [' + @db_name + '].sys.dm_db_partition_stats ps 
				GROUP BY ps.object_id, ps.index_id) AS ps_1
			LEFT OUTER JOIN 
				(SELECT 
					it.parent_id,
					SUM(ps.reserved_page_count) AS reserved,
					SUM(ps.used_page_count) AS used
					FROM [' + @db_name + '].sys.dm_db_partition_stats ps 
					INNER JOIN [' + @db_name + '].sys.internal_tables it  
					ON (it.object_id = ps.object_id)
					WHERE it.internal_type IN (202, 204)
					GROUP BY it.parent_id) AS it_ps_4 ON (it_ps_4.parent_id = ps_1.object_id)
			INNER JOIN [' + @db_name + '].sys.all_objects ao_2  
			ON ( ps_1.object_id = ao_2.object_id ) 
			INNER JOIN [' + @db_name + '].sys.indexes i 
			ON ao_2.object_id = i.object_id
			AND ps_1.index_id = i.index_id
			INNER JOIN [' + @db_name + '].sys.filegroups fg 
			ON i.data_space_id = fg.data_space_id
			INNER JOIN [' + @db_name + '].sys.schemas s_3  
			ON (ao_2.schema_id = s_3.schema_id)
			WHERE ao_2.type <> N''S'' and ao_2.type <> N''IT''
			ORDER BY s_3.name, ao_2.name
	'
			IF @debug = 1 PRINT '@sql = ' + @sql

			exec @result = sp_executesql @sql

			IF @result <> 0
				BEGIN
					RAISERROR('INSERT #db_table_index_files failed', 16, 1)
					RETURN
				END	
		END

	IF @debug = 1 select * from #storage_metrics order by [db_name], [table_name]

	SELECT *
	FROM #db_table_index_files
	ORDER BY [db_name], [table_name], [index_name]

	IF OBJECT_ID('tempdb..#db_stat_drive') IS NOT NULL
		DROP TABLE #db_stat_drive

	SELECT vs.logical_volume_name
	, vs.volume_mount_point
	, vs.file_system_type
	, AVG(CONVERT(DECIMAL(18,2), vs.total_bytes / 1073741824.0)) AS [Total Size (GB)]
	, AVG(CONVERT(DECIMAL(18,2), vs.available_bytes / 1073741824.0)) AS [Available Size (GB)]
	, AVG(CAST(CAST(vs.available_bytes AS FLOAT) / CAST(vs.total_bytes AS FLOAT) AS DECIMAL(18,2))) * 100 AS [Space Free %] 
	INTO #db_stat_drive
	FROM sys.master_files AS f 
	CROSS APPLY sys.dm_os_volume_stats(f.database_id, f.[file_id]) AS vs 
	GROUP BY vs.logical_volume_name
	, vs.volume_mount_point
	, vs.file_system_type
	ORDER BY vs.logical_volume_name
	OPTION (RECOMPILE);

	IF OBJECT_ID('tempdb..#db_stat_file') IS NOT NULL
		DROP TABLE #db_stat_file

	SELECT DB_NAME(database_id) AS [db_name]
	, Physical_Name
	, (CAST(size AS BIGINT) * 8)/1024 SizeMB
	, Name AS Logical_Name
	INTO #db_stat_file
	FROM sys.master_files 
	INNER JOIN @db_list dl
	ON DB_NAME(database_id) = dl.[db_name]
	ORDER BY [db_name], Physical_Name

	IF OBJECT_ID('tempdb..#db_stat_folder') IS NOT NULL
		DROP TABLE #db_stat_folder

	;WITH cte AS (SELECT DB_NAME(database_id) AS [db_name]
	, Folder_Name = SUBSTRING(Physical_Name, 1, LEN(Physical_Name) - CHARINDEX('\', REVERSE(Physical_Name), 1))
	, (CAST(size AS BIGINT) * 8)/1024 SizeMB
	, Name AS Logical_Name
	FROM sys.master_files 
	INNER JOIN @db_list dl
	ON DB_NAME(database_id) = dl.[db_name])

	SELECT Folder_Name, TotalSizeMB = SUM(SizeMB)
	INTO #db_stat_folder
	FROM cte
	GROUP BY Folder_Name
	ORDER BY Folder_Name

	--BEGIN: return results

	--db_stat_drive
	SELECT *
	FROM #db_stat_drive;

	--db_stat_file
	SELECT *
	FROM #db_stat_file

	--db_stat_folder
	SELECT *
	FROM #db_stat_folder

	--db_stat_table
	SELECT * 
	FROM #db_stat_table
	ORDER BY [db_name], [table_name], reserved_kb DESC
END TRY
BEGIN CATCH
	THROW;
END CATCH