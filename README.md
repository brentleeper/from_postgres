# from_postgres
Base engine for custom bulk data exports from PostgreSQL to one of many file types

Be sure to set up your DB connections in the sql_config.py file

usage: This tool exports data from PostGreSQL into one of several formats

        Pass option --help for more details.

       [-h] -et EXPORT_TYPE -ef EXPORT_FILE [-sf SQL_FILE] [-q QUERY]
       [-d DELIMITER] [-tn TABLE_NAME]

optional arguments:
 ```
  -h, --help            show this help message and exit
  -et EXPORT_TYPE, --export_type EXPORT_TYPE
                        Available Types: XL (Excel File), CSV (CSV File), JSON
                        (JSON Text File), SQLITE
  -ef EXPORT_FILE, --export_file EXPORT_FILE
                        The full path and file name of the export file
  -sf SQL_FILE, --sql_file SQL_FILE
                        Optionally specify a '.sql' file to run to preform the
                        export
  -q QUERY, --query QUERY
                        The query to run to collect data for export, required
                        if -sf option not provided
  -d DELIMITER, --delimiter DELIMITER
                        Optionally specify the CSV file delimiter, enclosed in
                        quotes. EXAMPLE: -d "|". Default is ',' (COMMA)NOTICE:
                        For ' ' pass TAB
  -tn TABLE_NAME, --table_name TABLE_NAME
                        Supply the table name for SQLITE export. Required when
                        using export_type SQLITE
```
