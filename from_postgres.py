import pandas as pd
from sqlalchemy import create_engine
from sql_config import psql_target_conn_str
import argparse
import time

parser = argparse.ArgumentParser("python3 from_postgres.py [options]\n\n\tThis tool exports data from PostGreSQL into one of several formats\n\n"
                                 "\tPass option --help for more details.\n")

parser.add_argument(
    "-et",
    "--export_type",
    required=True,
    help="Available Types: XL (Excel File), CSV (CSV File), JSON (JSON Text File), SQLITE"
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
    "-tn",
    "--table_name",
    required=False,
    help="Supply the table name for SQLITE export. Required when using export_type SQLITE"
)

user_args = parser.parse_args()

if psql_target_conn_str is None:
    print("Please set up PSQL connection in sql_config.py ")
    exit()
else:
    if user_args.query:
        query = user_args.query
    elif user_args.sql_file:
        with open(user_args.sql_file) as sql_file:
            query = sql_file.read()
    else:
        print("Please specify --query or --sql_file")
        exit()

    print("Establishing database connection.")

    try:
        psql_engine = create_engine(psql_target_conn_str)
    except:
        print("Unable to connect to database, please check the psql_target_conn_str in your sql_config.py file")
        exit()
    finally:
        print("Connected.")

if user_args.export_type.upper() in ["CSV", "XL", "SQLITE", "JSON"]:
    start_time = time.time()
    print("Extracting data.")
    data = pd.read_sql(query, psql_engine)
else:
    print("Invalid export_type.\nRun --help for usage information")
    exit()

record_ct = len(data)

if user_args.export_type.upper() == "XL":
    print(f"Writing {record_ct} records to Excel --> {user_args.export_file}")
    data.to_excel(user_args.export_file, index=False)

elif user_args.export_type.upper() == "CSV":
    print(f"Writing {record_ct} records to CSV --> {user_args.export_file}")
    if user_args.delimiter:
        if user_args.delimiter.upper() == "TAB":
            delimiter = "\t"
        else:
            delimiter = user_args.delimiter
    else:
        delimiter = ","

    print(f"Using delimiter '{delimiter}'")

    try:
        data.to_csv(user_args.export_file, sep=delimiter, index=False)
    except:
        print(f"ERROR: Bad delimiter --> {delimiter}")
        exit()

elif user_args.export_type.upper() == "JSON":
    print(f"Writing {record_ct} records to JSON --> {user_args.export_file}")
    data.to_json(user_args.export_file, orient="table")

elif user_args.export_type.upper() == "SQLITE":
    print(f"Writing {record_ct} records to SQLITE --> {user_args.export_file}")

    if user_args.table_name:
        print(f"Using table_name {user_args.table_name}")

        sql_lite_conn = lite_engine = create_engine(f"sqlite:///{user_args.export_file}").raw_connection()
        data.to_sql(user_args.table_name, sql_lite_conn, index=False)
    else:
        print("Missing required argument table_name.\nRun --help for usage information")
        exit()

end_time = time.time()

print(f"Finished in {round(end_time - start_time,2)} seconds")

print("Success.")
