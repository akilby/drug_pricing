from src.utils import get_mongo, PROJ_DIR
from src.tasks.histories import get_users
import os
import tqdm

coll = get_mongo()["drug_pricing"]["praw"]
users = get_users(coll, how='all')
users_fp = os.path.join(PROJ_DIR, "../data", "all_users.txt")

with open(users_fp, "w+") as users_file:
    for u in tqdm.tqdm(users):
        users_file.write(u)
