/* MIT License

Copyright (c) 2020 David McNamara

DESCRIPTION: This script creates a new script in the execute_jobs_sequentially_sql column that 
executes agent jobs in their scheduled order.  It will wait for each job to complete before executing the 
next one.

PURPOSE: A typical need for this would be when testing two or more agent jobs that must be 
executed in sequence but you don't want to run them manually or wait for their normally
scheduled execution.
*/

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

BEGIN TRY
	;WITH cte_jobs AS (SELECT j.name AS job_name
	, j.row_id
	, [next_run_date]
	, [next_run_time]
	, next_run_hour_minute = [next_run_time] / 100
	FROM (SELECT row_id = ROW_NUMBER() OVER (ORDER BY job_id), * FROM [msdb].[dbo].[sysjobs]) j
	inner join [msdb].[dbo].[sysjobschedules] js
	on j.job_id = js.job_id)

	SELECT *
	, sort_id = 1
	, execute_jobs_sequentially_sql = 'BEGIN TRY
		DECLARE @job_name VARCHAR(128) = ''' + j.job_name + '''
		DECLARE @job_id UNIQUEIDENTIFIER = (SELECT job_id FROM msdb.dbo.sysjobs WHERE [name] = @job_name)
		DECLARE @max_job_step INT = (SELECT MAX(step_id) FROM msdb.dbo.sysjobsteps WHERE job_id = @job_id)
		DECLARE @start_time datetime = GETDATE()
		DECLARE @status_message VARCHAR(1024)

		SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

		WAITFOR DELAY ''00:00:01''

		EXEC msdb.dbo.sp_start_job @job_id = @job_id

		WHILE (1 = 1)
		BEGIN
			IF EXISTS(SELECT 1
					FROM msdb.dbo.sysjobactivity
					WHERE job_id = @job_id
					AND last_executed_step_id = @max_job_step
					AND ISNULL(stop_execution_date, ''1/1/1900'') >= @start_time)
				BEGIN
					SET @status_message = @job_name + '' completed''
					RAISERROR(@status_message, 10, 1) WITH NOWAIT
					BREAK
				END	
			ELSE
				BEGIN
					SET @status_message = @job_name + '' is running''
					RAISERROR(@status_message, 10, 1) WITH NOWAIT
				END		

			WAITFOR DELAY ''00:00:02''
		END
	END TRY
	BEGIN CATCH
		PRINT ''agent_job_gen_seq_exec_script failed'';
		THROW;
	END CATCH

	' 
	FROM cte_jobs j
		UNION
	SELECT *
	, sort_id = 2
	, execute_jobs_sequentially_sql = 'GO'
	FROM cte_jobs j
	ORDER BY [next_run_date], next_run_hour_minute, row_id, sort_id
	
	SELECT *
	FROM [msdb].[dbo].[sysjobs]	

	SELECT *
	FROM [msdb].dbo.sysschedules

	SELECT j.name
	, [next_run_date]
	, [next_run_time]
	, job_schedule_enabled = s.enabled
	, job_interval_type = CASE s.freq_type WHEN 1 THEN 'One time only' 
	WHEN 4 THEN  'Daily' 
	WHEN 8 THEN 'Weekly' 
	WHEN 16 THEN 'Monthly '
	WHEN 32 THEN 'Monthly, relative to freq_interval'
	WHEN 64 THEN 'Runs when the SQL Server Agent service starts' 
	WHEN 128 THEN 'Runs when the computer is idle' END
	, job_interval_subtype = s.freq_subday_type
	, job_interval_frequency = s.freq_interval
	, job_interval_subfrequency = s.freq_subday_interval
	, job_frequency_recurrence = s.freq_recurrence_factor
	, next_run_hour_minute = [next_run_time] / 100
	, execute_job_sql = 'EXEC dbo.sp_start_job N''' + j.name + ''''
	FROM [msdb].[dbo].[sysjobs] j
	inner join [msdb].[dbo].[sysjobschedules] js
	on j.job_id = js.job_id
	inner join [msdb].dbo.sysschedules s
	on js.schedule_id = s.schedule_id
	ORDER BY [next_run_date], next_run_hour_minute
END TRY
BEGIN CATCH
	PRINT 'agent_job_gen_seq_exec_script failed';
	THROW;
END CATCH