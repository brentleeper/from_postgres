import argparse
import time
from psql_exporter import PSQLExporter
from sql_config import psql_target_conn_str

parser = argparse.ArgumentParser("python3 from_postgres.py [options]\n\n\tThis tool exports data from PostGreSQL into"
                                 " one of several formats\n\n\tPass option --help for more details.\n")

parser.add_argument(
    "-et",
    "--export_type",
    required=False,
    help="Default: CSV. Available Types: XL (Excel File), CSV (CSV File), JSON (JSON Text File), SQLITE"
)
parser.add_argument(
    "-ef",
    "--export_file",
    required=True,
    help="The full path and file name of the export file"
)
parser.add_argument(
    "-sf",
    "--sql_file",
    required=False,
    help="Optionally specify a '.sql' file to run to preform the export"
)
parser.add_argument(
    "-q",
    "--query",
    required=False,
    help="The query to run to collect data for export, required if -sf option not provided"
)
parser.add_argument(
    "-d",
    "--delimiter",
    required=False,
    help="Optionally specify the CSV file delimiter, enclosed in quotes. EXAMPLE -> -d '|' <-  Default is ',' (COMMA) "
         "NOTICE: For '\\t' pass TAB"
)

parser.add_argument(
    '-nh',
    "--no_headers",
    action='store_true',
    required=False,
    help="Optionally pass this flag to exclude headers from the export file for type CSV"
)

parser.add_argument(
    "-tn",
    "--table_name",
    required=False,
    help="Supply the table name for SQLITE export. Required when using export_type SQLITE"
)

parser.add_argument(
    '-c',
    "--compress",
    action='store_true',
    required=False,
    help="Optionally pass this flag to compress the export into a zip file"
)

user_args = parser.parse_args()

exporter = PSQLExporter(psql_target_conn_str)

exporter.set_query(query=user_args.query, sql_file=user_args.sql_file)

export_type =  user_args.export_type

if not export_type:
    export_type = "CSV"

exporter.set_export_type(export_type, delimiter=user_args.delimiter, table_name=user_args.table_name)

start_time = time.time()

success = exporter.get_data()

if not success:
    print(exporter.error)
    exit()

headers = not(user_args.no_headers == True)

exporter.do_export(user_args.export_file, compress=user_args.compress, headers=headers)

end_time = time.time()

print(f"Finished in {round(end_time - start_time,2)} seconds")

print("Success.")
