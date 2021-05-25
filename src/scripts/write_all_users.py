import os

import tqdm

from src.tasks.histories import get_users
from src.utils import PROJ_DIR, get_mongo


def main():
    coll = get_mongo()["drug_pricing"]["praw"]
    users = get_users(coll, how="all")
    users_fp = os.path.join(PROJ_DIR, "clutter", "users_to_write.txt")

    with open(users_fp, "w+") as users_file:
        for u in tqdm.tqdm(users):
            if u:
                users_file.write(str(u) + "\n")


if __name__ == "__main__":
    main()
