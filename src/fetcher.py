from concurrent.futures import ProcessPoolExecutor
from requests.exceptions import (
    RequestException,
    ConnectionError,
    HTTPError,
    Timeout,
    JSONDecodeError,
)
from .utils import (
    fetch_data,
    persist_data_to_json_file
)


class Fetcher:

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.total_number_of_records = 0
        self.filenames = []
        self.errors = []

    def get_metadata(self):
        try:
            response = fetch_data(self.start_date, self.end_date, start=0, rows=1)
            return response.json()
        except Timeout as err:
            err.args = ("Connection timed out. Please try again",)
            raise
        except ConnectionError as err:
            err.args = ("Connection to the server failed. Please check your internet connection.",)
            raise
        except HTTPError as err:
            err.args = (f"{err.args[0]}. Please try again.",)
            raise
        except RequestException as err:
            err.args = ("An unexpected error occurred. Please try again",)
            raise

    def generate_patent_record_sequence(self):
        return [value for value in range(0, self.total_number_of_records, 100)]

    def download_patent_data(self, start):
        rows = 100
        if (start + rows) > self.total_number_of_records:
            rows = self.total_number_of_records - start
        try:
            print(f"Downloading patent records {start + 1}...{start + rows}")
            response = fetch_data(self.start_date, self.end_date, start=start, rows=rows)
            data = response.json()["results"]
            filename = f"tmp/patent_data_{start + 1}_{start + rows}.json"
            persist_data_to_json_file(filename, data)
            return True, filename
        except Exception:
            return False, f"Failed to download patent data from {start + 1} to {start + rows}"

    def process(self):
        metadata = self.get_metadata()
        self.total_number_of_records = metadata.get("recordTotalQuantity", 0)

        if not self.total_number_of_records:
            print("They are no patent records for the specified date range.")
            return

        patent_record_sequence = self.generate_patent_record_sequence()

        with ProcessPoolExecutor() as executor:
            results = executor.map(self.download_patent_data, patent_record_sequence)

            for success, result in results:
                if success:
                    self.filenames.append(result)
                else:
                    self.errors.append(result)

        return self.filenames, self.errors



