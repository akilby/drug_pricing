# Drug Pricing Project
This project attempts to extract street opiate pricing and location data from online Reddit forums.

## Setup Instructions

### 1. Environment variables
This project utilizes the python "dotenv" package to read secret information as environment variables.

For security reasons, the file containing these environment is not tracked on git.  In order to make the git version of the project functional:

A. Create a file named `.env` in the project root
B. Store the following variables in there with your own credentials/info
- RUSERNAME: your Reddit username
- RPASSWORD: your Reddit password
- RCLIENT_ID: your Reddit API client id
- RSECRET_KEY: your Reddit API secret key
- RUSER_AGENT: the identifier you use for your machine
- PORT: the port you plan to run mongo on (8000 is default)
- DB_NAME: the name of your mongo database
- COLL_NAME: the name of your mongo collection for the raw data

Checkout [here](https://www.reddit.com/dev/api/oauth/) for help with Reddit API setup.

### 2. Install dependencies
Create a virtual environment if you want.

Then, run `pip install -r requirements.txt`.

## Project Structure

`notebooks/` contains exploratory code in Jupyter Notebooks

`tasks/` contains files that define Luigi tasks for the data pipeline
- `mongo.py` contains tasks that import data to a mongo database
- `read_data.py` contains tasks that parse data from Reddit and files

`utils/` contains utility functions and objects referenced/utilized throughout the project

`tests/` contains all tests for the project

## Testing

To run project tests, run `python -m unittest`

**Note:** tests involving loading data from files will not run using code from git because sample data is not committed.
In order to get these to run, create and populate the following two directories with appropriate sample data:
- `<project_root>/sample_data/comments`
- `<project_root>/sample_data/threads`
