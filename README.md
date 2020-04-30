# Drug Pricing Scheduler
This project contains code for extracting posts and comments from Reddit. It allows for extraction from various subreddits and over flexible periods of time. It persists all retrieved data in a MongoDB database.

## Project Structure

`scheduler.py` allows for command-line execution of the scheduler

`pipeline.py` contains functions & objects for the scheduling pipeline

`utils.py` contains project-wide variables and functions

`tests/` contains all tests for the project

## Setup Instructions

### 1. Environment variables
This project utilizes the python "dotenv" package to read secure information as environment variables. This secure information is not tracked on version control, so you must populate your own credentials. Follow the below instructions.

A. Create a file named `.env` in the project root

B. Store the following variables in there with your own credentials/info
- RUSERNAME: your Reddit username
- RPASSWORD: your Reddit password
- RCLIENT_ID: your Reddit API client id
- RSECRET_KEY: your Reddit API secret key
- RUSER_AGENT: the identifier you use for your machine
- PORT: the port you plan to run mongo on (27017 is default)
- DB_NAME: the name of your mongo database
- COLL_NAME: the name of your mongo collection for the raw data

Checkout [here](https://www.reddit.com/dev/api/oauth/) for help with Reddit API setup.

### 2. Install dependencies
Run `pipenv install` to install all dependencies

## Running the Project

**Running the scheduler**

To update all new data from all subreddits, run `make run`.

More specific execution of the scheduler is also possible. Run `python3 scheduler.py --help` for more details.

**Running tests**

Run `make test` to execute all tests.

*Note:* tests involving loading data from files will not run using code from git because sample data is not maintained in version control.
