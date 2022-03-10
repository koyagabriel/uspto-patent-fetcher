import click
from src import Fetcher


@click.command()
@click.argument("start_date")
@click.argument("end_date")
def patent_fetcher(start_date, end_date):
    try:
        fetcher = Fetcher(start_date, end_date)
        fetcher.process()
        print("Done")
    except Exception as error:
        print(error)


if __name__ == "__main__":
    patent_fetcher()

