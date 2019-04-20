import pandas as pd
from sqlalchemy import create_engine
from sql_config import psql_target_conn_str
import traceback


class PsqlExporter:
    def __init__(self):
        if psql_target_conn_str is None:
            print("PsqlExporter: Please set up PSQL connection in sql_config.py ")
            exit()
        else:
            print("PsqlExporter: Establishing database connection.")

            try:
                self.psql_engine = create_engine(psql_target_conn_str)
            except:
                print("PsqlExporter: Unable to connect to database, please check the psql_target_conn_str in your sql_config.py file")
                exit()
            finally:
                print("PsqlExporter: Connected.")

        self.avail_types = ["CSV", "XL", "SQLITE", "JSON"]
        self.type = None
        self.data = None
        self.error = None
        self.record_ct = None
        self.delimiter = ","
        self.table_name = None
        self.query = None

    def set_export_type(self, type, delimiter=None, table_name=None):
        if type.upper() in self.avail_types:
            self.type = type

            if type.upper() == "SQLITE" and table_name is not None:
                self.table_name = table_name
            elif type.upper() == "SQLITE" and self.table_name is None and table_name is None:
                print("PsqlExporter: Missing required param table_name for type: SQLITE")
                exit()
            elif type == "CSV" and delimiter:
                self.delimiter = delimiter

        else:
            print(f"PsqlExporter: Invalid export_type.\nAvailable Types --> {self.avail_types}")
            exit()

    def set_query(self, query=None, sql_file=None):
        if not sql_file and query:
            self.query = query
        elif sql_file:
            with open(sql_file) as file:
                self.query = file.read()


    def get_data(self, query=None):
        if not query:
            query = self.query

        if query:
            try:
                self.data = pd.read_sql(query, self.psql_engine)
                self.record_ct = len(self.data)
                return True
            except:
                self.error = traceback.format_exc()
                return False
        else:
            print("PsqlExporter: Must set or pass query.")
            exit()

    def set_delimiter(self,delimiter):
        self.delimiter = delimiter

    def set_table_name(self,table_name):
        self.table_name = table_name

    def do_export(self, file_path):
        if self.type.upper() == "XL":
            print(f"PsqlExporter: Writing {self.record_ct} records to Excel --> {file_path}")
            self.data.to_excel(file_path, index=False)

        elif self.type.upper() == "CSV":
            print(f"PsqlExporter: Writing {self.record_ct} records to CSV --> {file_path}")
            if self.delimiter:
                if self.delimiter.upper() == "TAB":
                    delimiter = "\t"
                else:
                    delimiter = self.delimiter
            else:
                delimiter = ","

            print(f"PsqlExporter: Using delimiter '{delimiter}'")

            try:
                self.data.to_csv(file_path, sep=self.delimiter, index=False)
            except:
                print(f"PsqlExporter: ERROR: Bad delimiter --> {delimiter}")
                exit()

        elif self.type.upper() == "JSON":
            print(f"PsqlExporter: Writing {self.record_ct} records to JSON --> {file_path}")
            self.data.to_json(file_path, orient="table")

        elif self.type.upper() == "SQLITE":
            print(f"PsqlExporter: Writing {self.record_ct} records to SQLITE --> {file_path}")
            print(f"PsqlExporter: Using table_name {self.table_name}")

            sql_lite_conn = create_engine(f"sqlite:///{file_path}").raw_connection()
            self.data.to_sql(self.table_name, sql_lite_conn, index=False)

