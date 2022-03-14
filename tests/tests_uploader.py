import os
import unittest
from sqlalchemy import create_engine, inspect
from sqlalchemy_utils import create_database, database_exists, drop_database
from dotenv import load_dotenv
from src.uploader import DatabaseUploader

load_dotenv()
url = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('DB_HOST', 'db')}:5432/test_database"


class DatabaseUploaderTestCase(unittest.TestCase):

    def setUp(self):
        load_dotenv()
        self.database_config = {
            "host": os.getenv("DB_HOST", "db"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "username": os.getenv("POSTGRES_USER"),
            "database": "test_database",
            "port": os.getenv("DB_PORT", 5432)
        }
        self.filename = ["sample.json"]

    @classmethod
    def setUpClass(cls):
        if not database_exists(url):
            create_database(url)

    @classmethod
    def tearDownClass(cls):
        if database_exists(url):
            drop_database(url)

    def test_instantiation_with_empty_filenames(self):
        with self.assertRaises(ValueError, msg="There is nothing to upload into the database."):
            DatabaseUploader([], self.database_config)

    def test_instantiation_without_username(self):
        with self.assertRaises(ValueError, msg="Database username is missing, please provide a username."):
            del self.database_config["username"]
            DatabaseUploader(self.filename, self.database_config)

    def test_instantiation_without_password(self):
        with self.assertRaises(ValueError, msg="Database password is missing, please provide a password."):
            del self.database_config["password"]
            DatabaseUploader(self.filename, self.database_config)

    def test_instantiation_without_database_name(self):
        with self.assertRaises(ValueError, msg="Database name is missing, please provide a database name."):
            del self.database_config["database"]
            DatabaseUploader(self.filename, self.database_config)

    def test_database_connection_with_correct_credentials(self):
        db_uploader = DatabaseUploader(self.filename, self.database_config)
        self.assertTrue(db_uploader.test_database_connection())

    def test_database_connection_with_wrong_credentials(self):
        err_msg = " * Could not connect to the database server. Please check your database configuration"
        with self.assertRaises(Exception, msg=err_msg):
            self.database_config.update({"password": "wrong_password"})
            DatabaseUploader(self.filename, self.database_config).test_database_connection()

    def test_create_table_method(self):
        db_uploader = DatabaseUploader(self.filename, self.database_config)
        conn = create_engine(db_uploader.database_url)
        self.assertFalse(inspect(conn).has_table("patents_test_table"))
        db_uploader.create_table("patents_test_table")
        self.assertTrue(inspect(conn).has_table("patents_test_table"))



