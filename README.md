# Drug Pricing Scheduler
This project contains code for extracting posts and comments from Reddit. It allows for extraction from various subreddits and over flexible periods of time. It persists all retrieved data in a MongoDB database.

## Setup Instructions

### 1. Environment variables
This project utilizes the python "dotenv" package to read sensitive information as environment variables. This information is not tracked on version control, so you populate your own credentials by following the below instructions.

A. Create a file named `.env` in the project root

B. Store the following variables in there with your own credentials/info
**Reddit credentials**
- RUSERNAME: your Reddit username
- RPASSWORD: your Reddit password
- RCLIENT_ID: your Reddit API client id
- RSECRET_KEY: your Reddit API secret key
- RUSER_AGENT: the identifier you use for your machine

**Mongo credentials**
- PORT: the port you plan to run mongo on (27017 is default)
- DB_NAME: the name of your mongo database
- HOST: the database hostname
- MUSERNAME: the username for the database
- MPASSWORD: the password for the database

Checkout [here](https://www.reddit.com/dev/api/oauth/) for help with Reddit API setup.

### 2. Install dependencies
Run `pipenv install` to install all dependencies

## Running the Project

**Updating the database**

To update all new data from all subreddits, run `make run`.


**Running tests**

Run `make test` to execute all tests.

*Note:** tests involving loading data from files will not run using code from git because sample data is not maintained in version control.

**Additional Functionality**

Other available functionality include:
- Retrieve Reddit posts by subreddit and timeframe
- Retrieve all posts from the subreddits listed in `utils.py` file since the last date in the mongo db
- Add spacy objects to all posts in the mongo db

Run `python -m src.__init__ --help` for more details.

