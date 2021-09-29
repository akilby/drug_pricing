from src.utils import get_praw, get_psaw, sub_comm_to_post, posts_to_mongo, connect_to_mongo, dt_to_utc, utc_to_dt
from datetime import date, datetime
from collections import Counter
import pickle


def cache_month_counts(posts, is_sub):
    months = []
    for p in posts:
        dt = utc_to_dt(p.created_utc)
        month = (dt.month, dt.year)
        months.append(month)
    
    month_counts = Counter(months)
    ts = datetime.now()
    name = "sub" if is_sub else "comm"
    fp = f"cache/psaw_{name}_month_counts_{ts}.pk"
    with open(fp, 'wb') as f:
        pickle.dump(month_counts, f)


def main():
    connect_to_mongo()
    r = get_praw()
    api = get_psaw(r)

    start = int(dt_to_utc(datetime(2018, 8, 1)).timestamp())
    end = int(dt_to_utc(datetime.now()).timestamp())
    print('Start:', start)
    print('End:', end)

    print('Querying submissions .....')
    subs_query = api.search_submissions(subreddit='opiates', after=start, before=end)
    subs = list(subs_query)
    print(f"{len(subs)} submissions retrieved.")

    print('Querying comments .....')
    comms_query = api.search_comments(subreddit='opiates', after=start, before=end)
    comms = list(comms_query)
    print(f"{len(comms)} comments retrieved.")

    print('Caching month counts .....')
    cache_month_counts(subs, True)
    cache_month_counts(comms, False)

    print('Converting to Posts .....')
    sub_posts = sub_comm_to_post(subs, True)
    comm_posts = sub_comm_to_post(comms, False)

    print('Storing in DB .....')
    posts_to_mongo(sub_posts + comm_posts)

    print('Done.')


if __name__ == '__main__':
    main()