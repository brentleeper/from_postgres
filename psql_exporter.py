import pandas as pd
import psycopg2
import sqlite3
import traceback
from os.path import basename
from zipfile import ZipFile, ZIP_DEFLATED


class PSQLExporter:
    def __init__(self, ppg2_con_str):
        """
        :param ppg2_con_str: psycopg2 connection string
        """

        print("PSQLExporter: Establishing database connection.")

        try:
            self.psql_engine = psycopg2.connect(ppg2_con_str)
        except:
            print("PSQLExporter: Unable to connect to database.")
            exit()
        else:
            print("PSQLExporter: Connected.")

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
                print("PSQLExporter: Missing required param table_name for type: SQLITE")
                exit()
            elif type == "CSV" and delimiter:
                self.delimiter = delimiter

        else:
            print(f"PSQLExporter: Invalid export_type.\nAvailable Types --> {self.avail_types}")
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
                print("PSQLExporter: Extracting data.")
                self.data = pd.read_sql(query, self.psql_engine)
                self.record_ct = len(self.data)
                return True
            except:
                self.error = traceback.format_exc()
                return False
        else:
            print("PSQLExporter: Must set or pass query.")
            exit()

    def set_delimiter(self,delimiter):
        """
        Use this to set the delimiter separately from setting the export type
        :param delimiter: The delimiter to use for type CSV export
        :return: None
        """
        self.delimiter = delimiter

    def do_sql(self, query=None, sql_file=None, query_list=None):
        """
        Run prep sql or cleanup sql. Pass only 1 of the 3 params
        :param query: Optional. The query to run against your database for prep.
                      Required if sql_file not passed
        :param sql_file: Optional. The .sql file containing a query or set of queries to run.
                      Required if query not passed
        :param query_list: Optional. A list of queries to run in order against your database for prep
        :return: True on success. False on failure
        """

        if (query and sql_file and query_list) or (not query and not sql_file and not query_list):
            self.error = "Must pass exactly 1 param"
            return False

        if query or sql_file:
            if query:
                self.query = query
            elif sql_file:
                with open(sql_file) as file:
                    self.query = file.read()


            print("PSQLExporter: Doing prep/cleanup SQL.")

            try:
                self.psql_engine.execute(query)
                self.psql_engine.commit()
                return True
            except:
                self.error = traceback.format_exc()
                return False
        elif query_list:
            for q in query_list:
                if not self.do_sql(query=q):
                    if self.error == "Must pass exactly 1 param":
                        self.error = "Queries in query_list must not be None or False"
                    return False

    def set_table_name(self,table_name):
        """
        Use this to set the table name separately from setting the export type
        :param table_name: The name of the table within the SQLITE file.
        :return: None
        """
        self.table_name = table_name

    def do_export(self, file_path, compress=False, headers=True):
        """
        Write the extracted data to the given file path using the set export type and optionally compress the file.
        :param file_path: The file path of where to write the exported data.
        :param compress: Optional. Pass True to compress the exported data. This will remove the uncompressed file.
        :param headers: Default True, Pass False to exclude headers from CSV exports
        :return: None
        """

        file_path = file_path.replace("\\\\", "\\")
        file_path = file_path.replace("\\","/")

        if self.type.upper() == "XL":
            print(f"PSQLExporter: Writing {self.record_ct} records to Excel --> {file_path}")
            self.data.to_excel(file_path, index=False, engine='xlsxwriter')

        elif self.type.upper() == "CSV":
            print(f"PSQLExporter: Writing {self.record_ct} records to CSV --> {file_path}")
            if self.delimiter:
                if self.delimiter.upper() == "TAB":
                    delimiter = str("\t")
                    print("PSQLExporter: Using delimiter 'TAB'")
                else:
                    delimiter = self.delimiter
                    print(f"PSQLExporter: Using delimiter '{delimiter}'")
            else:
                delimiter = ","
                print(f"PSQLExporter: Using delimiter '{delimiter}'")

            if headers:
                print(f"PSQLExporter: With headers")
            else:
                print(f"PSQLExporter: Without headers")

            try:
                self.data.to_csv(file_path, sep=delimiter, index=False, header=headers)
            except:
                print(f"PSQLExporter: ERROR: Bad delimiter --> '{delimiter}'")
                exit()

        elif self.type.upper() == "JSON":
            print(f"PSQLExporter: Writing {self.record_ct} records to JSON --> {file_path}")
            self.data.to_json(file_path, orient="table")

        elif self.type.upper() == "SQLITE":
            print(f"PSQLExporter: Writing {self.record_ct} records to SQLITE --> {file_path}")
            print(f"PSQLExporter: Using table_name {self.table_name}")

            sql_lite_conn = sqlite3.connect(file_path)
            self.data.to_sql(self.table_name, sql_lite_conn, index=False)

        if compress:
            base_path = "/".join(file_path.split("/")[:-1])

            if base_path != "":
                base_path += "/"

            base_name = basename(file_path)
            zip_file = base_path + base_name.split(".")[0] + ".zip"

            print(f"PSQLExporter: Compressing to {zip_file}")

            zip = ZipFile(zip_file, "w", ZIP_DEFLATED)
            zip.write(file_path, base_name)
            zip.close()
