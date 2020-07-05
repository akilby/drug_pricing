import unittest
from typing import Dict

from src.tasks.histories import get_users_histories
from src.utils import DB_NAME, TEST_COLL_NAME, get_mongo, get_praw, get_psaw


class TestGetUsersHistories(unittest.TestCase):
    """Tests for retrieving full posting histories for multiple users."""

    coll = get_mongo()[DB_NAME][TEST_COLL_NAME]
    praw = get_praw()
    psaw = get_psaw(praw)


if __name__ == '__main__':
    unittest.main()
