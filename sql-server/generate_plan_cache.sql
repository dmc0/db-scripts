/*
MIT License

Copyright (c) 2023 music.iii.vi.ii.v

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

David McNamara
Version 0.12

@plan_cache_count_goal should be the only value you need to set.
Set it to the number of plan caches you want to create.
However, be aware that is limited by your SQL Server memory and
other configuration settings.

References for plan cache
https://www.sqlskills.com/blogs/erin/sql-server-plan-cache-limits/
https://support.microsoft.com/en-us/topic/kb3026083-fix-sos-cachestore-spinlock-contention-on-ad-hoc-sql-server-plan-cache-causes-high-cpu-usage-in-sql-server-798ca4a5-3813-a3d2-f9c4-89eb1128fe68
https://learn.microsoft.com/en-us/previous-versions/tn-archive/cc293624(v=technet.10)?redirectedfrom=MSDN
https://learn.microsoft.com/en-us/sql/relational-databases/performance-monitor/sql-server-plan-cache-object?view=sql-server-ver16

"guaranteed" to be unique table names are used
following the pattern plan_cache_source_lvfkjyodeszmskaxrpck_0
where _0 is incremented for each new table needed.

-- create a special database for this if you would like
CREATE DATABASE plan_cache_load_test

-- Trace flags 174 and 8302 only work on startup
-- To set them for a Linux SQL Server container it appears you have to use mssql.conf
-- Instructions for that are here
https://www.nocentino.com/posts/2021-09-12-configuring-sql-server-in-containers/

-- check trace status
DBCC TRACESTATUS
*/

BEGIN TRY
    -- how many plan caches you want to create.  Max for this version of the script is 1,000,000
    DECLARE @plan_cache_count_goal INT = 100000
    DECLARE @debug bit = 0
    /* how many rows you want to insert into plan_cache_source.
    100 should be plenty.  More than that slows down each query. */
    DECLARE @row_count_target INT = 100
    DECLARE @column_count INT = 400
    DECLARE @plan_cache_count_goal_limit INT = 1000000
    DECLARE @plan_cache_per_source_table INT = 75000 -- normally 75000
    DECLARE @source_table_prefix NVARCHAR(128) = 'plan_cache_source_lvfkjyodeszmskaxrpck_'
    DECLARE @source_table_count_needed INT = 0
    DECLARE @queries_executed_count INT = 0
    DECLARE @table_pos INT = 0
    DECLARE @table_row_pos INT = 0
    DECLARE @row_pos INT = 0
    DECLARE @column_pos INT = 0
    DECLARE @col_pos_1 INT = 0
    DECLARE @col_pos_2 INT = 0
    -- Have to use NVARCHAR(4000) because SQL Server still suffers from a sporadic NVARCHAR(MAX) truncation / sizing problem
    DECLARE @sql_1 NVARCHAR(4000) = ''
    DECLARE @sql_2 NVARCHAR(4000) = ''
    DECLARE @msg NVARCHAR(2000)
    DECLARE @result Int = 0
    DECLARE @max_int INT
    DECLARE @col_1 VARCHAR(16) -- column names are short in this code
    DECLARE @col_2 VARCHAR(16)

    -- value needs to be 0.  We WANT inefficient ad hoc settings
    IF EXISTS (SELECT 1
               FROM sys.configurations t
               WHERE name = 'optimize for ad hoc workloads'
                 and t.value = 0)
        RAISERROR ('OK.  optimize for ad hoc workloads = 0', 10, 1) WITH NOWAIT
    ELSE
        RAISERROR ('ERROR: value for ad hoc workloads must be 0.   We WANT it to be inefficient for the purposes of generating plan cache.', 16, 1)

    IF @plan_cache_count_goal > @plan_cache_count_goal_limit
        BEGIN
            SET @msg = CONCAT('@plan_cache_count_goal cannot be greater than ', @plan_cache_count_goal_limit,
                              ' based on the current limitations in this script')
            RAISERROR (@msg, 16, 1) WITH NOWAIT
        END

    IF @column_count > 400
        BEGIN
            SET @msg =
                    '@column_count cannot be greater than 400 because of the NVARCHAR 4000 limit for defining objects dynamically'
            RAISERROR (@msg, 16, 1) WITH NOWAIT
        END

    IF @row_count_target > 100
        BEGIN
            SET @msg = '@row_count_target should not need to be > 100 for the purposes of this utility'
            RAISERROR (@msg, 16, 1) WITH NOWAIT
        END

    SET @source_table_count_needed = @plan_cache_count_goal / @plan_cache_per_source_table

    -- add 1 to the source table count needed if the division as not even
    IF @plan_cache_count_goal % @plan_cache_per_source_table != 0
        SET @source_table_count_needed += 1

    SET @table_pos = 0
    SET @column_pos = 0

    WHILE @table_pos < @source_table_count_needed
        BEGIN
            SET @table_pos += 1

            SET @msg = CONCAT('Setting up table ', @source_table_prefix, @table_pos, '...')
            RAISERROR (@msg, 10, 1) WITH NOWAIT

            -- drop it every time because it does not take long to create
            IF OBJECT_ID(CONCAT(@source_table_prefix, @table_pos), 'U') IS NOT NULL
                BEGIN
                    SET @sql_1 = CONCAT('DROP TABLE ', @source_table_prefix, @table_pos)
                    exec @result = sp_executesql @sql_1

                    IF @result <> 0
                        BEGIN
                            SET @msg = CONCAT('FAILED: ', @sql_1)
                            RAISERROR (@msg, 16, 1)
                            RETURN
                        END
                END

            -- create and load the source table for the queries
            IF OBJECT_ID(CONCAT(@source_table_prefix, @table_pos), 'U') IS NULL
                BEGIN
                    SET @column_pos = 0

                    SET @sql_1 = CONCAT('CREATE TABLE ', @source_table_prefix, @table_pos,
                                        '([row_id] int IDENTITY(1, 1) PRIMARY KEY ', char(10))

                    WHILE @column_pos < @column_count
                        BEGIN
                            SET @column_pos +=1

                            SET @sql_1 += CONCAT(',c', @column_pos, ' INT')
                        END

                    SET @sql_1 += ')'

                    IF @debug = 1 PRINT @sql_1

                    exec @result = sp_executesql @sql_1

                    IF @result <> 0
                        BEGIN
                            SET @msg = CONCAT('FAILED: ', @sql_1)
                            RAISERROR (@msg, 16, 1)
                            RETURN
                        END

                    SET @column_pos = 0

                    SET @sql_1 = CONCAT('INSERT INTO ', @source_table_prefix, @table_pos, '(')
                    SET @sql_2 = 'SELECT '

                    WHILE @column_pos < @column_count
                        BEGIN
                            SET @column_pos +=1

                            --- insert columns
                            SET @sql_1 += CONCAT('c', @column_pos, ',')
                            -- select columns
                            SET @sql_2 += CONCAT(@column_pos, ',')
                        END

                    -- remove trailing comma
                    SET @sql_1 = LEFT(@sql_1, LEN(@sql_1) - 1)
                    SET @sql_2 = LEFT(@sql_2, LEN(@sql_2) - 1)

                    SET @sql_1 += CONCAT(')', char(10))
                    SET @sql_2 += CONCAT(char(10), ' from generate_series(1,', @row_count_target, ', 1) s', char(10))

                    SET @sql_1 += @sql_2

                    IF @debug = 1 PRINT @sql_1

                    exec @result = sp_executesql @sql_1

                    IF @result <> 0
                        BEGIN
                            RAISERROR ('insert into table: sp_executesql @sql_1 failed', 16, 1)
                            RETURN
                        END
                END
        END

    SET @table_pos = 0

    WHILE @table_pos < @source_table_count_needed
        BEGIN
            SET @table_pos += 1

            SET @msg = CONCAT('Querying table ', @source_table_prefix, @table_pos, '...')
            RAISERROR (@msg, 10, 1) WITH NOWAIT

            SET @table_row_pos = 0
            SET @row_pos = 0
            SET @col_pos_1 = 1 -- start with first column
            SET @col_pos_2 = 1 -- start with first column and it will move to the second

            WHILE @table_row_pos < @plan_cache_per_source_table
                BEGIN
                    SET @col_pos_2 += 1
                    SET @row_pos += 1
                    SET @queries_executed_count += 1

                    IF @queries_executed_count > @plan_cache_count_goal
                        RETURN

                    IF @row_pos > @row_count_target
                        SET @row_pos = 1

                    IF @col_pos_2 >= @column_count
                        BEGIN
                            SET @col_pos_1 += 1
                            SET @col_pos_2 = @col_pos_1 + 1
                        END

                    IF @col_pos_2 > @column_count
                        BREAK

                    -- increment @table_row_pos after check for BREAK
                    SET @table_row_pos += 1

                    SET @col_1 = CONCAT('c', @col_pos_1)
                    SET @col_2 = CONCAT('c', @col_pos_2)

                    SET @sql_1 = CONCAT('SELECT @max_int_out = row_id
					FROM ', @source_table_prefix, @table_pos, '
					WHERE ', @col_1, ' = ', @row_pos,
                                        ' AND ', @col_2, ' = ', @row_pos)

                    -- setting to a variable to avoid result sets in SQL IDE
                    exec @result = sp_executesql @sql_1, N'@max_int_out int OUTPUT', @max_int_out=@max_int

                    IF @result <> 0
                        BEGIN
                            SET @msg = CONCAT('SELECT failed: ', @sql_1)
                            RAISERROR (@msg, 16, 1)
                            RETURN
                        END

                    IF @queries_executed_count % 500 = 0
                        BEGIN
                            SET @msg = CONCAT('Query exec number ', @queries_executed_count)
                            RAISERROR (@msg, 10, 1) WITH NOWAIT
                        END
                END
        END

    SET @msg = CONCAT('Processing has completed! ', @queries_executed_count, ' queries were executed.')
    RAISERROR (@msg, 10, 1) WITH NOWAIT
END TRY
BEGIN CATCH
    THROW;
END CATCH


-- These helper queries are not meant to execute with the script
-- Select and execute sections of them as needed
IF 1 = 2
    BEGIN
        -- The sum of all the cache sizes.  It is limited by the amount of memory SQL Server is able to use.
        select sum(cast(size_in_bytes as bigint)) as all_cached_plans_size_in_bytes
        from sys.dm_exec_cached_plans;

        SELECT (select count(distinct plan_handle) from sys.dm_exec_query_stats)     as [unique query-stats.plan-handle],
               (select count(distinct query_hash) from sys.dm_exec_query_stats)      as [unique query-stats.query-hash],
               (select count(distinct query_plan_hash) from sys.dm_exec_query_stats) as [unique query-stats.query-plan-hash],
               (select count(distinct plan_handle) from sys.dm_exec_cached_plans)    as [unique cached-plans.plan-handle]

        SELECT (select count(1) from sys.dm_exec_query_stats)     as query_stats,
               (select count(1) from sys.dm_exec_cached_plans)    as cached_plans,
               (select count(1) from sys.dm_exec_procedure_stats) as procedure_stats

        select * from sys.dm_exec_query_stats;
        select * from sys.dm_exec_cached_plans;
        select * from sys.dm_exec_procedure_stats;

        select t.physical_memory_kb, committed_kb from sys.dm_os_sys_info t

        select SUM(CAST(cntr_value as bigint)) as cntr_value from sys.dm_os_performance_counters

        select count(1)
        from sys.columns t
        where t.object_id = OBJECT_ID('plan_cache_source_lvfkjyodeszmskaxrpck', 'U')

        select name, type, buckets_count
        from sys.dm_os_memory_cache_hash_tables
        where name IN ('SQL Plans', 'Object Plans', 'Bound Trees')

        select name, type, pages_kb, entries_count
        from sys.dm_os_memory_cache_counters
        where name IN ('SQL Plans', 'Object Plans', 'Bound Trees')
    END
