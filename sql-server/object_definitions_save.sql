/* MIT License

Copyright (c) 2020 David McNamara

DESCRIPTION: 
    This script will save the object definitions of the types of objects below to a table.  
    However, you must enter each object name individually in the VALUES for the @obj_names table.

    You also need to specify the name of the database (@obj_def_save_db)
and table (@obj_def_save_table) where you want the object definitions
to be saved.  The database must already exist but the table will be created for you if
it does not already exist.  You can also use the same obj_def_save_table value repeatedly
and the object definitions will be appended to the table.

    The main advantages to saving object definitions this way are:
        1) They are less likely to be lost.
        2) A hash value is also created for them which makes it more difficult to tamper with
		their saved definitions.

Object types whose definitions can be saved.

D = DEFAULT (stand-alone only)
FN = SQL scalar function
IF = SQL inline table-valued function
P = SQL Stored Procedure
R = Rule (old-style, stand-alone)
RF = Replication-filter-procedure
S = System base table
TF = SQL table-valued-function
TR = SQL DML trigger
V = View
*/

DECLARE @save_desc VARCHAR(1024) = 'Backup of objects before change'
DECLARE @obj_def_save_table VARCHAR(128) = 'obj_def_2020_1020'

DECLARE @obj_names TABLE(RowOrderId INT IDENTITY(1, 1), obj_name NVARCHAR(128))
INSERT @obj_names (obj_name) VALUES ('p1')
, ('p2')
DECLARE @obj_def_save_db VARCHAR(128) = 'msdb'  --msdb is recommended

SET NOCOUNT ON

BEGIN TRY
	DECLARE @obj_name NVARCHAR(128) 
	DECLARE @substring_hash_val VARBINARY(8000)
	DECLARE @hash_val VARBINARY(8000) = 0x
	DECLARE @def NVARCHAR(MAX)
	DECLARE @def_len INT
	DECLARE @def_char_pos INT = 1
	DECLARE @row_ord_pos INT = 0
	DECLARE @obj_cnt INT = 0
	DECLARE @obj_id INT
	DECLARE @err_msg VARCHAR(2000)
	DECLARE @user_msg VARCHAR(2000)
	DECLARE @sql NVARCHAR(MAX) = ''
	DECLARE @exec_result INT = 0
	DECLARE @obj_def_save_table_full_name VARCHAR(384) = QUOTENAME(@obj_def_save_db) + '.[dbo].' + QUOTENAME(@obj_def_save_table)

	SELECT @obj_cnt = COUNT(*) FROM @obj_names

	--first check to make sure all the objects listed exist in the current database
	WHILE @row_ord_pos < @obj_cnt
		BEGIN
			SET @row_ord_pos = @row_ord_pos + 1

			SELECT @obj_name = obj_name
			FROM @obj_names
			WHERE RowOrderId = @row_ord_pos

			SET @obj_id = OBJECT_ID(@obj_name)

			IF @obj_id IS NULL
				BEGIN
					SET @err_msg = 'There is no object named "' +  @obj_name + '" in the ' + DB_NAME() + ' database'
					RAISERROR(@err_msg, 16, 1)
				END		
		END

	SET @row_ord_pos = 0

	IF OBJECT_ID(@obj_def_save_table_full_name) IS NULL
	BEGIN
		SET @sql = 'CREATE TABLE ' + @obj_def_save_table_full_name + '(
		row_id INT IDENTITY(1, 1)
		, save_desc VARCHAR(1024)
		, exec_datetime datetime
		, obj_name VARCHAR(128)
		, user_name VARCHAR(128)
		, hash_val VARBINARY(8000)
		, abbr_hash_val VARBINARY(8000)
		, hash_val_len INT
		, obj_id INT
		, definition NVARCHAR(MAX)
		, uses_ansi_nulls BIT
		, uses_quoted_identifier BIT
		, is_schema_bound BIT
		, uses_database_collation BIT
		, is_recompiled BIT
		, null_on_null_input BIT
		, execute_as_principal_id BIT)'

		BEGIN TRY
			exec @exec_result = sp_executesql @sql

			IF @exec_result <> 0
				BEGIN
					SET @err_msg = 'FAILED to create the object definition save table named ' +  @obj_def_save_table_full_name + ' in the ' + @obj_def_save_db + ' database'
					RAISERROR(@err_msg, 16, 1)
				END
			ELSE
				BEGIN
					SET @user_msg = 'Successfully created the object definition save table named ' +  @obj_def_save_table_full_name + ' in the ' + @obj_def_save_db + ' database'
					RAISERROR(@user_msg, 10, 1)
				END			
		END TRY
		BEGIN CATCH
				SET @err_msg = 'FAILED to create the object definition save table named ' +  @obj_def_save_table_full_name + ' in the ' + @obj_def_save_db + ' database
				Error Number:' + CAST(ERROR_NUMBER() AS VARCHAR(24)) + ', ' + ISNULL(ERROR_MESSAGE(), '')

				RAISERROR(@err_msg, 16, 1)
		END CATCH
	END

	WHILE @row_ord_pos < @obj_cnt
		BEGIN
			SET @row_ord_pos = @row_ord_pos + 1	

			SELECT @obj_name = obj_name
			FROM @obj_names
			WHERE RowOrderId = @row_ord_pos

			SET @obj_id = OBJECT_ID(@obj_name)

			SELECT @def = [definition]
			FROM sys.sql_modules
			WHERE [object_id] = @obj_id

			SET @def_len = LEN(@def)
			SET @hash_val = 0x
			SET @def_char_pos = 1

			WHILE @def_char_pos < @def_len
			BEGIN
				SET @substring_hash_val = HASHBYTES('SHA2_512', SUBSTRING(@def, @def_char_pos, 4000))
	
				SET @hash_val = @hash_val + @substring_hash_val

				SET @def_char_pos = @def_char_pos + 4000
			END

			SET @sql = 'INSERT ' + @obj_def_save_table_full_name + ' (save_desc
			, exec_datetime
			, obj_name
			, user_name
			, hash_val
			, abbr_hash_val
			, hash_val_len
			, obj_id
			, definition
			, uses_ansi_nulls
			, uses_quoted_identifier
			, is_schema_bound
			, uses_database_collation
			, is_recompiled
			, null_on_null_input
			, execute_as_principal_id)' + CHAR(13) + CHAR(10)

			SET @sql = @sql + 'SELECT save_desc = @save_desc
			, exec_datetime = GETDATE()
			, obj_name = @obj_name
			, user_name = SYSTEM_USER
			, hash_val = @hash_val
			, abbr_hash_val = CASE WHEN LEN(CAST(@hash_val AS VARCHAR(MAX))) <= 8000 THEN HASHBYTES(''SHA2_512'', CAST(@hash_val AS VARCHAR(MAX))) ELSE 0x END
			, hash_val_len = LEN(@hash_val)
			, object_id
			, definition
			, uses_ansi_nulls
			, uses_quoted_identifier
			, is_schema_bound
			, uses_database_collation
			, is_recompiled
			, null_on_null_input
			, execute_as_principal_id
			FROM sys.sql_modules
			WHERE object_id = @obj_id'

			BEGIN TRY
				exec @exec_result = sp_executesql @sql, N'@hash_val VARBINARY(8000), @save_desc VARCHAR(1204), @obj_id INT, @obj_name VARCHAR(128)', @hash_val = @hash_val, @save_desc = @save_desc, @obj_id = @obj_id, @obj_name = @obj_name

				IF @exec_result <> 0
					BEGIN
						SET @err_msg = 'FAILED to save object named ' +  @obj_name + ' in the ' + DB_NAME() + ' databaseto ' + @obj_def_save_table_full_name
						RAISERROR(@err_msg, 16, 1)
					END
				ELSE
					BEGIN
						SET @user_msg = 'Successfully saved the object named ' +  @obj_name + ' in the ' + DB_NAME() + ' database to ' + @obj_def_save_table_full_name
						RAISERROR(@user_msg, 10, 1)
					END			
			END TRY
			BEGIN CATCH
					SET @err_msg = 'FAILED to save object named ' +  @obj_name + ' in the ' + DB_NAME() + ' database to ' + @obj_def_save_table_full_name + '
					Error Number:' + CAST(ERROR_NUMBER() AS VARCHAR(24)) + ', ' + ISNULL(ERROR_MESSAGE(), '')
					RAISERROR(@err_msg, 16, 1)

			END CATCH				
		END

		EXEC('SELECT * FROM ' + @obj_def_save_table_full_name)
END TRY
BEGIN CATCH
	PRINT 'object_definitions_save failed';
	THROW;
END CATCH
