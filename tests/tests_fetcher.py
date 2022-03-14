import os
import unittest
from unittest.mock import patch, Mock
from requests.exceptions import (
    Timeout,
    ConnectionError,
    HTTPError,
    RequestException
)
from src.fetcher import Fetcher
from .helpers import get_patent_response_sample


class FetcherTestCase(unittest.TestCase):

    def setUp(self):
        self.fetcher = Fetcher("2001-04-25", "2001-05-25", dirname="tests/tmp")
        self.record_total_quantity = 6503

    def get_response(self, url, params=None):
        response_mock = Mock()
        response_mock.status_code = 200
        response_mock.json.return_value = get_patent_response_sample(self.record_total_quantity)
        return response_mock

    def test_generate_patent_record_sequence(self):
        self.fetcher.total_number_of_records = 2500
        result = self.fetcher.generate_patent_record_sequence()
        self.assertEqual(len(result), 25)

    def test_get_metadata_timeout(self):
        with patch("src.utils.requests") as mock_requests:
            mock_requests.get.side_effect = Timeout
            with self.assertRaises(Timeout, msg="Connection timed out. Please try again"):
                self.fetcher.get_metadata()

    def test_get_metadata_connection_error(self):
        with patch("src.utils.requests") as mock_requests:
            mock_requests.get.side_effect = ConnectionError
            with self.assertRaises(ConnectionError, msg="Connection to the server failed. Please check your internet connection."):
                self.fetcher.get_metadata()

    def test_get_metadata_http_error(self):
        with patch("src.utils.requests") as mock_requests:
            mock_requests.get.side_effect = HTTPError("Bad request!")
            with self.assertRaises(HTTPError, msg="Bad Request!. Please try again"):
                self.fetcher.get_metadata()

    def test_get_metadata_request_exception(self):
        with patch("src.utils.requests") as mock_requests:
            mock_requests.get.side_effect = RequestException
            with self.assertRaises(RequestException, msg="An unexpected error occurred. Please try again"):
                self.fetcher.get_metadata()

    def test_download_patent_data(self):
        with patch("src.utils.requests") as mock_requests:
            mock_requests.get.side_effect = self.get_response
            success, filename = self.fetcher.download_patent_data(0)
            self.assertTrue(success)
            self.assertTrue(os.path.exists(filename))

    def test_process_with_zero_total_number_of_records(self):
        self.record_total_quantity = 0
        with patch("src.utils.requests") as mock_requests:
            mock_requests.get.side_effect = self.get_response
            with self.assertRaises(ValueError, msg="They are no patent records for the specified date range."):
                self.fetcher.process()

    def test_process_with_adequate_number_of_records(self):
        self.record_total_quantity = 2
        with patch("src.utils.requests") as mock_requests:
            mock_requests.get.side_effect = self.get_response
            filenames, errors = self.fetcher.process()
            self.assertListEqual(filenames, ["tests/tmp/patent_data_1_2.json"])
            self.assertListEqual(errors, [])
