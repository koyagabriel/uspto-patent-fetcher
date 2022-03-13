import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from sqlalchemy import (
    create_engine, inspect, MetaData,
    Table, Column, Integer, String
)
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool


class DatabaseUploader:
    DATABASE_UPLOAD_ERROR = "Database upload failed!"
    COLUMN_MAPPER = {
        "patentNumber": "patent_number",
        "patentApplicationNumber": "patent_application_number",
        "assigneeEntityName": "assignee_entity_name",
        "filingDate": "filing_date",
        "grantDate": "grant_date",
        "inventionTitle": "invention_title"
    }

    def __init__(self, filenames, database_config, table_name="patents"):
        self.filenames = filenames
        self.host = database_config.get("host", "localhost")
        self.port = database_config.get("port", 5432)
        self.username = database_config.get("username")
        self.password = database_config.get("password")
        self.database = database_config.get("database")
        self.table_name = table_name
        self.errors = []

        if not filenames:
            raise ValueError("There is nothing to upload into the database.")

        if not self.username:
            raise ValueError(f"{self.DATABASE_UPLOAD_ERROR}! Database username is missing, please provide a username.")

        if not self.password:
            raise ValueError(f"{self.DATABASE_UPLOAD_ERROR}! Database password is missing, please provide a password")

        if not self.database:
            raise ValueError(f"{self.DATABASE_UPLOAD_ERROR}! Database name is missing, please provide oa database name")

        self.database_url = f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

        self.test_database_connection()

    def test_database_connection(self):
        try:
            conn = create_engine(self.database_url)
            conn.connect()
            conn.dispose()
        except OperationalError as err:
            err.args = (f"{self.DATABASE_UPLOAD_ERROR}! Could not connect to the database server. "
                        f"Please check your database configuration",)
            raise

    def create_table(self, table_name):
        conn = create_engine(self.database_url)
        metadata_obj = MetaData()
        if not inspect(conn).has_table(table_name):
            patents = Table(table_name, metadata_obj,
                            Column("id", Integer, primary_key=True),
                            Column("patent_number", String()),
                            Column("patent_application_number", String()),
                            Column("assignee_entity_name", String()),
                            Column("filing_date", String()),
                            Column("grant_date", String()),
                            Column("invention_title", String()),
                            )
            patents.create(conn)
            conn.dispose()

    def upload_patent_data(self, filename):
        start, end = filename.split(".")[0].split("_")[-2:]
        try:
            df = pd.read_json(filename).rename(columns=self.COLUMN_MAPPER)
            df = df[list(self.COLUMN_MAPPER.values())]
            con = create_engine(self.database_url, poolclass=NullPool)
            df.to_sql(self.table_name, con=con, if_exists="append", index=False, method="multi")
            con.dispose()
            print(f"Successfully saved patent record from {start} to {end} into the database.")
            return True, None
        except Exception as err:
            print(err)
            return False, f"Failed to save patent record from {start} to {end} into the database"

    def process(self):
        self.create_table(self.table_name)
        with ProcessPoolExecutor() as executor:
            results = executor.map(self.upload_patent_data, self.filenames)

            for success, result in results:
                if not success:
                    self.errors.append(result)
