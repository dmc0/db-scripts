/* MIT License

Copyright (c) 2020 David McNamara

DESCRIPTION: This script will generate the bcp export commands for you for multiple tables.
Copy and paste the command_line column values into a shell window or run them in a script.
*/

DECLARE @output_folder NVARCHAR(512) = 'D:\D_Temp\'
DECLARE @db_name NVARCHAR(128) = 'DWConfiguration'
DECLARE @use_trusted_connection BIT = 0
DECLARE @sql_login_name NVARCHAR(128) = 'sa'  --will be ignored if @use_trusted_connection = 1

DECLARE @file_name_suffix NVARCHAR(128) = FORMAT(GETUTCDATE(), '_yyyyMMdd_hhmmss_') + 'UTC'
DECLARE @server_name NVARCHAR(512) = CAST(SERVERPROPERTY('SERVERNAME') AS NVARCHAR(512))

DECLARE @table_names TABLE (table_name NVARCHAR(128))
INSERT @table_names VALUES ('node')  
, ('compute_node')
, ('')
, ('')

SELECT [command_line] = 'bcp "dbo.' + tn.table_name + '" out "' + @output_folder + tn.table_name + @file_name_suffix + '.dat"' 
	+ ' -S "' + @server_name + '" -d "' + @db_name + '" -w' 
	+ CASE @use_trusted_connection WHEN 1 THEN ' -T' ELSE ' -U "' + @sql_login_name + '"' END
FROM @table_names tn
WHERE LEN(ISNULL([table_name], '')) > 0
