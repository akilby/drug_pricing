import unittest

from src.utils import get_praw, get_psaw


class TestGetUsersHistories(unittest.TestCase):
    """Tests for retrieving full posting histories for multiple users."""

    praw = get_praw()
    psaw = get_psaw(praw)


if __name__ == '__main__':
    unittest.main()
