import os
from typing import List

from src.schema import Post
from src.tasks.csv import COMM_COLNAMES, SUB_COLNAMES, extract_csv
from src.utils import PROJ_DIR
from tests.tasks.test_praw import TestExtractPraw


class TestExtractCsv(TestExtractPraw):
    """
    Tests for the read_all_files method.

    This object inherits from the object for testing the extract_posts method.
    Thus, the same tests are run except posts to be used are overriden here.
    """

    # file paths that test data are stored in
    data_dir = os.path.join(PROJ_DIR, "..", "data")

    def __get_posts(self) -> List[Post]:
        """Overwrite the method for retrieving posts to read from files."""
        thread_fp = os.path.join(self.data_dir, "all_subms.csv")
        comm_fp = os.path.join(self.data_dir, "all_comments.csv")
        threads = extract_csv(thread_fp, SUB_COLNAMES)
        comments = extract_csv(comm_fp, COMM_COLNAMES)
        return threads + comments
