# USPTO PATENT FETCHER
A CLI program that calls the USPTO's API and saves patent data for all granted patents granted between two dates provided as CLI arguments.

# Solution
This program majorly consist of two phases of operation:
* The fetching/downloading phase which is achieved by the Fetcher Component.
* The uploading phase which is achieved by the Uploader Component.

### Fetching Phase
The fetcher component makes multiple api calls to USPTO in parallel to fetch batches of patent records based on a generated batch sequence.
These records are saved in a temporary directory in json formats for later processing by the uploader component.

### Uploading Phase
This phase is handled by the DatabaseUploader component. It reads the patent json files created by the Fetcher component in batches,
selects the required keys/fields needed and persist them in a Postgres database. This operation can be achieved offline since the 
patent records were saved locally.

###### Some libraries used to achieve this cli program are:
1. Click: used for creating beautiful command line interfaces.
2. SQLAlchemy: an ORM  used for interacting with the database.
3. Pandas:  used to process the json files by leveraging on the DataFrame object.
4. request: used for making api calls to USPTO.

## Installation
1. Ensure you have docker installed on your machine.
2. Clone the repository.
3. Create a `.env` file in the root dir with environment variables as shown in `.env.example` file.
4. Run `docker-compose up --build` to build images and spin necessary containers.
5. To start/run the app, run the command `docker-compose run patent_fetcher 2017-01-01 2017-01-03`.
6. To run the test suites, run the command `docker-compose run tests`.


## Authors
Koya Adegboyega.