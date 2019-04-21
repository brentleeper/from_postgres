import pandas as pd
import psycopg2
import sqlite3
from sql_config import psql_target_conn_str
import traceback
import os
from os.path import basename
from zipfile import ZipFile, ZIP_DEFLATED

class PsqlExporter:
    def __init__(self):
        """
            Must configure psql_target_conn_str within the sql_config file.
        """
        if psql_target_conn_str is None:
            print("PsqlExporter: Please set up PSQL connection in sql_config.py ")
            exit()
        else:
            print("PsqlExporter: Establishing database connection.")

            try:
                self.psql_engine = psycopg2.connect(psql_target_conn_str)
            except:
                print("PsqlExporter: Unable to connect to database, please check the psql_target_conn_str in your sql_config.py file")
                exit()
            else:
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
        """
        Set the export type for this export.
        :param type: The export type. Options ["CSV", "XL", "SQLITE", "JSON"]
        :param delimiter: Optional. The delimiter to use when exporting with type CSV. Default ','
        :param table_name: Optional. The name of the table within the SQLITE file. Required for type SQLITE
        :return: None
        """
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
        """
        Set the query to use for data extraction.
        :param query: Optional. The query to run against your database for data extraction.
                      Required if sql_file not passed
        :param sql_file: Optional. The .sql file containing a query or set of queries to run.
                      Required if query not passed
        :return: None
        """
        if not sql_file and query:
            self.query = query
        elif sql_file:
            with open(sql_file) as file:
                self.query = file.read()


    def get_data(self, query=None):
        """
        Retrieve the data from the configured database using the supplied or set query.
        :param query: Optional. The query to run against your database for data extraction. Supersedes set_query.
        :return: True on success. False on failure.
        """
        if not query:
            query = self.query

        if query:
            try:
                print("PsqlExporter: Extracting data.")
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
        """
        Use this to set the delimiter separately from setting the export type
        :param delimiter: The delimiter to use for type CSV export
        :return: None
        """
        self.delimiter = delimiter

    def set_table_name(self,table_name):
        """
        Use this to set the table name separately from setting the export type
        :param table_name: The name of the table within the SQLITE file.
        :return: None
        """
        self.table_name = table_name

    def do_export(self, file_path, compress=False):
        """
        Write the extracted data to the given file path using the set export type and optionally compress the file.
        :param file_path: The file path of where to write the exported data.
        :param compress: Optional. Pass True to compress the exported data. This will remove the uncompressed file.
        :return: None
        """

        file_path = file_path.replace("\\\\", "\\")
        file_path = file_path.replace("\\","/")

        if self.type.upper() == "XL":
            print(f"PsqlExporter: Writing {self.record_ct} records to Excel --> {file_path}")
            self.data.to_excel(file_path, index=False, engine='xlsxwriter')

        elif self.type.upper() == "CSV":
            print(f"PsqlExporter: Writing {self.record_ct} records to CSV --> {file_path}")
            if self.delimiter:
                if self.delimiter.upper() == "TAB":
                    delimiter = str("\t")
                    print("PsqlExporter: Using delimiter 'TAB'")
                else:
                    delimiter = self.delimiter
                    print(f"PsqlExporter: Using delimiter '{delimiter}'")
            else:
                delimiter = ","
                print(f"PsqlExporter: Using delimiter '{delimiter}'")

            try:
                self.data.to_csv(file_path, sep=delimiter, index=False)
            except:
                print(f"PsqlExporter: ERROR: Bad delimiter --> '{delimiter}'")
                exit()

        elif self.type.upper() == "JSON":
            print(f"PsqlExporter: Writing {self.record_ct} records to JSON --> {file_path}")
            self.data.to_json(file_path, orient="table")

        elif self.type.upper() == "SQLITE":
            print(f"PsqlExporter: Writing {self.record_ct} records to SQLITE --> {file_path}")
            print(f"PsqlExporter: Using table_name {self.table_name}")

            sql_lite_conn = sqlite3.connect(file_path)
            self.data.to_sql(self.table_name, sql_lite_conn, index=False)

        if compress:
            base_path = "/".join(file_path.split("/")[:-1])

            if base_path != "":
                base_path += "/"

            base_name = basename(file_path)
            zip_file = base_path + base_name.split(".")[0] + ".zip"

            print(f"PsqlExporter: Compressing to {zip_file}")

            zip = ZipFile(zip_file, "w", ZIP_DEFLATED)
            zip.write(file_path, base_name)
            zip.close()

            os.remove(file_path)

