/* MIT License

Copyright (c) 2020 David McNamara

DESCRIPTION: This script will return all privileges GRANTED on all objects for all users.  
It does not return implied privileges, e.g., those privileges sysadmins and schema owners have.  
If an object has had no privileges to it granted, the [user_name] column will be '[[user_name]]'
 
REQUIREMENTS: You must execute the script against the database name for which you want to retrieve privileges.
*/

SET NOCOUNT ON
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

BEGIN TRY
	DECLARE @sql as NVARCHAR(4000)
	DECLARE @obj_name as NVARCHAR(128)
	DECLARE @owner as NVARCHAR(128)
	DECLARE @id INT
	DECLARE @uid INT

	/* #sql_privileges is rooted in this select statement
	select p.action & ~convert(INT, 0x10000000), N'column' = [col_name](p.id, p.colid), p.uid, N'[user_name]' = [user_name](p.uid),
		   p.protect_type, o.name, N'owner' = [user_name](o.uid), p.id, N'grantor' = [user_name](p.grantor)
	from #output p, dbo.sysobjects o
	where o.id = p.id
	order by p.uid, p.id, p.protect_type, p.action
	*/

	IF OBJECT_ID('tempdb..#sql_privileges') IS NOT NULL
		DROP TABLE #sql_privileges

	CREATE TABLE #sql_privileges (
		[action]      INT  NOT NULL,
		[col_name] varchar(128) NULL,
		[uid]         INT  NOT NULL,
		[user_name] varchar(128) NULL,
		protect_type INT  NOT NULL,
		obj_name varchar(128) NULL,
		[owner] varchar(128) NULL,
		[id]          INT  NOT NULL,
		grantor varchar(128) NULL
	)

	DECLARE object_cursor CURSOR LOCAL FORWARD_ONLY FAST_FORWARD READ_ONLY FOR 
	SELECT so.[id], so.[name] , so.uid, su.name
	FROM sysobjects so inner join sysusers su on so.uid = su.uid
	WHERE xtype IN('U', 'P', 'V', 'IF', 'FN')
	AND so.[name] NOT IN ('dm_cryptographic_provider_algorithms', 'dm_cryptographic_provider_keys', 'dm_cryptographic_provider_sessions')
	ORDER BY xtype, so.[name]

	OPEN object_cursor

	FETCH NEXT FROM object_cursor 
	INTO @id, @obj_name, @uid, @owner

	BEGIN TRY
		WHILE @@FETCH_STATUS = 0
			BEGIN
				SET @sql = 'exec sp_MSobjectprivs ''' + @obj_name + ''', N''object'''  --, null, null, null, null, 0, 1'
		
				INSERT #sql_privileges EXECUTE sp_executesql @sql
		
				IF @@ROWCOUNT = 0
				INSERT #sql_privileges VALUES(0, NULL, @uid, '', 0, @obj_name, @owner, @id, NULL)

				FETCH NEXT FROM object_cursor 
				INTO @id, @obj_name, @uid, @owner
			END
	END TRY
	BEGIN CATCH
		IF ERROR_NUMBER() NOT IN (15001) --ignore 15001 because it's an object like dm_cryptographic_provider_algorithms
			BEGIN
				PRINT 'privileges_all_objects_get failed';
				THROW;
			END		
	END CATCH

	CLOSE object_cursor
	DEALLOCATE object_cursor

	SELECT DISTINCT sql_grant = 'GRANT ' + (CASE [action] WHEN 193 THEN 'SELECT' WHEN 195 THEN 'INSERT' WHEN 197 THEN 'UPDATE' 
	WHEN 196 THEN 'DELETE' WHEN 26 THEN 'DRI' WHEN 224 THEN 'EXECUTE' WHEN 0 THEN CAST([action] AS VARCHAR(12)) ELSE CAST([action] AS varchar(64)) END) + ' ON [' + obj_name + '] TO [[user_name]]'
	, (CASE so.xtype WHEN 'U' THEN 'TABLE' WHEN 'P' THEN 'PROCEDURE' 
	WHEN 'V' THEN 'VIEW' WHEN 'IF' THEN 'FUNCTION' WHEN 'FN' THEN 'FUNCTION' ELSE so.xtype END) AS obj_type
	, p.[owner]
	, obj_name
	, [user_name]
	, (CASE [action] WHEN 193 THEN 'SELECT' WHEN 195 THEN 'INSERT' WHEN 197 THEN 'UPDATE' 
	WHEN 196 THEN 'DELETE' WHEN 26 THEN 'DRI' WHEN 224 THEN 'EXECUTE' WHEN 0 THEN CAST([action] AS VARCHAR(12)) ELSE CAST([action] AS varchar(64)) END) AS permission
	FROM #sql_privileges p INNER JOIN sysobjects so on p.id = so.id
	ORDER BY obj_type, obj_name	
END TRY
BEGIN CATCH
	PRINT 'privileges_all_objects_get failed';
	THROW;

	CLOSE object_cursor
	DEALLOCATE object_cursor
END CATCH
GO
