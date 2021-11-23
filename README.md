# Location inference on social media data for agile monitoring of public health crises

## Overview
This project contains the code for work done in [this](https://arxiv.org/abs/2111.01778) paper. There are three primary components:
1. [Data collection/processing](#data-collection)
2. [Location Inference](#location-inference)
3. [Analysis](#analysis)

## Setup Instructions

Follow the below instructions before attempting to run anything within the project. You can optionally utilize the Makefile to automate install portions of this process by running `make setup`.

The `virtualenv` package is recommended for managing a virtual environment as this is what was used for development. 

### 1. Environment variables
This project utilizes the python `dotenv` package to read sensitive information as environment variables.  **For this project to run properly, you need to:** fill in your own credentials in the `.sample-env` file and rename the file as `.env`. 

Checkout [here](https://www.reddit.com/dev/api/oauth/) for help with Reddit API setup if needed.

### 2. Install dependencies
Run `pip install -r requirements.txt` to install all dependencies

### 3. Install Spacy language model
Run `python -m spacy download en_core_web_sm`

## Components
The entrance point for running programs is the `src.__main__` file.  The full list of available command line functionality is displayed below.  Specific and relevant commands are also discussed in more detail in their corresponding subsections.

You can optionally use commands in a provided Makefile to simplify program running.


**Command Line Functionality**

```
usage: __main__.py [-h] [--subr SUBR] [--startdate STARTDATE] [--enddate ENDDATE] [--limit LIMIT] [--csv CSV] [--posttype POSTTYPE] [--lastdate] [--update] [--histories] [--spacy]

optional arguments:
  -h, --help            show this help message and exit

Praw Querying:
  --subr SUBR           The subreddit to use.
  --startdate STARTDATE
                        The start date for Praw scraping
  --enddate ENDDATE     The end date for Praw scraping
  --limit LIMIT         The number of Praw objects to limit querying

CSV Parsing:
  --csv CSV             The csv filepath to parse from
  --posttype POSTTYPE   If data to parse is submissions (s) or comments (c)

Tasks:
  --lastdate            Retrieve the last date stored in the mongo collection.
  --update              Insert all posts for all subreddits from the last posted date
  --histories           Retrieve full posting history for all users.
  --spacy               Run spacy on all new documents.

Location Inference:
  --infer-users INFER_USERS
                        Provide a csv filepath containing usernames (one on each line) to perform location inference on.
```


### Data Collection
The primary component here is a scheduler that allows for extraction from various subreddits and over flexible periods of time. It persists all retrieved data in a MongoDB database.  
### Location Inference
To perform location inference on a specific set of users, run the command `python -m src --infer-users <filepath>` where `<filepath>` is a path to a csv containing line separated usernames.  The output will be writted to a pickle file.
