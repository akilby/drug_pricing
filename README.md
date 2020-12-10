# Drug Pricing Scheduler
This project contains code for extracting posts and comments from Reddit. It allows for extraction from various subreddits and over flexible periods of time. It persists all retrieved data in a MongoDB database.

## Setup Instructions

### 1. Environment variables
This project utilizes the python `dotenv` package to read sensitive information as environment variables.  **For this project to run properly, you need to:** fill in your own credentials in the `.sample-env` file and rename the file as `.env`. 

Checkout [here](https://www.reddit.com/dev/api/oauth/) for help with Reddit API setup if needed.

### 2. Install dependencies
Run `pipenv install` to install all dependencies

## Running the Project

**Updating the database**

To update all new data from all subreddits, run `make run args=--update`.


**Running tests**

Run `make test` to execute all tests.

*Note:** tests involving loading data from files will not run using code from git because sample data is not maintained in version control.

**Additional Functionality**

Other available functionality include:
- Retrieve Reddit posts by subreddit and timeframe
- Retrieve all posts from the subreddits listed in `utils.py` file since the last date in the mongo db
- Add spacy objects to all posts in the mongo db

Run `make run args=--help` for more details.

