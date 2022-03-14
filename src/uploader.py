import click
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from sqlalchemy import (
    create_engine, inspect, MetaData,
    Table, Column, Integer, String
)
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool


class DatabaseUploader:
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
            raise ValueError("Database username is missing, please provide a username.")

        if not self.password:
            raise ValueError("Database password is missing, please provide a password")

        if not self.database:
            raise ValueError("Database name is missing, please provide oa database name")

        self.database_url = f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

        self.test_database_connection()

    def test_database_connection(self):
        try:
            click.secho(" => Testing database connection!!!")
            conn = create_engine(self.database_url)
            conn.connect()
            conn.dispose()
            click.secho(" => Connection established!!!\n", fg="green")
            return True
        except OperationalError as err:
            err.args = (" * Could not connect to the database server. "
                        f"Please check your database configuration",)
            raise

    def create_table(self, table_name):
        click.secho(f"Creating database table {table_name} if it doesn't exist!!!")
        conn = create_engine(self.database_url)
        metadata_obj = MetaData()
        try:
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
                click.secho(f" => Successfully created {table_name} table.\n", fg="green")
                return
        except Exception as err:
            err.args = (f" * An error occurred while trying to create {table_name} table", )
            raise
        click.secho(f" => The table named {table_name} already exist in the database\n", fg="green")

    def upload_patent_data(self, filename):
        start, end = filename.split(".")[0].split("_")[-2:]
        try:
            df = pd.read_json(filename).rename(columns=self.COLUMN_MAPPER)
            df = df[list(self.COLUMN_MAPPER.values())]
            con = create_engine(self.database_url, poolclass=NullPool)
            df.to_sql(self.table_name, con=con, if_exists="append", index=False, method="multi")
            con.dispose()
            click.secho(f" => Successfully inserted patent record from {start} to {end} into the database.", fg="green")
            return True, None
        except Exception:
            error_msg = f" * Failed to insert patent record from {start} to {end} into the database"
            click.secho(error_msg, fg="red")
            return False, error_msg

    def process(self):
        self.create_table(self.table_name)
        click.secho("Starting database insertion of patent records ....")
        with ProcessPoolExecutor() as executor:
            results = executor.map(self.upload_patent_data, self.filenames)

            for success, result in results:
                if not success:
                    self.errors.append(result)
