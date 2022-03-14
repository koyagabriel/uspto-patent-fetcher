import os
import click
from dotenv import load_dotenv
from src.fetcher import Fetcher
from src.uploader import DatabaseUploader


@click.command()
@click.argument("start_date")
@click.argument("end_date")
def patent_fetcher(start_date, end_date):
    click.clear()
    click.secho("Welcome to Patent Fetcher!", fg="green")
    try:
        click.secho("Fetching Patent Records from USPTO....\n")
        fetcher = Fetcher(start_date, end_date)
        filenames, errors = fetcher.process()
        click.secho("\nCompleted fetching records from USPTO!!!\n")

        click.clear()
        click.secho("Uploading patent records to database\n")
        load_dotenv()
        database_config = {
            "host": os.getenv("DB_HOST", "db"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "username": os.getenv("POSTGRES_USER"),
            "database": os.getenv("POSTGRES_DB"),
            "port": os.getenv("DB_PORT", 5432)
        }
        database_uploader = DatabaseUploader(filenames, database_config, table_name="patents")
        database_uploader.process()
        click.secho("\nUpload completed")

    except Exception as error:
        click.secho(f" * {error}", fg="red")


if __name__ == "__main__":
    patent_fetcher()

