/* MIT License

Copyright (c) 2020 David McNamara

DESCRIPTION: This script will generate the bcp export commands for you for multiple tables.
Copy and paste the command_line column values into a shell window or run them in a script.
*/

--import data using bcp
DECLARE @db_name NVARCHAR(128) = 'DWConfiguration'
DECLARE @use_trusted_connection BIT = 1
DECLARE @sql_login_name NVARCHAR(128) = 'sa'  --will be ignored if @use_trusted_connection = 1

DECLARE @src_file_dest_table TABLE (source_file_name NVARCHAR(1024), dest_table_name NVARCHAR(128))
INSERT @src_file_dest_table VALUES ('D:\D_Temp\node_20201023_121242_UTC.dat', 'node')
, ('', '')
, ('', '')
, ('', '')

DECLARE @server_name NVARCHAR(512) = CAST(SERVERPROPERTY('SERVERNAME') AS NVARCHAR(512))

SELECT [command_line] = 'bcp "dbo.' + tn.dest_table_name + '" in "' + tn.source_file_name + '"'
	+ ' -S "' + @server_name + '" -d "' + @db_name + '" -w'
	+ CASE @use_trusted_connection WHEN 1 THEN ' -T' ELSE ' -U "' + @sql_login_name + '"' END
FROM @src_file_dest_table tn
WHERE LEN(ISNULL(source_file_name , '')) > 0
