import tqdm

from src.utils import connect_to_mongo, get_praw, utc_to_dt
from src.schema import Post, SubmissionPost, CommentPost

from mongoengine.queryset.visitor import Q
from praw import Reddit
from praw.models import Submission, Comment
from typing import List, Dict
import requests


def fill_missing_post_fields(post: Post, praw: Reddit):
    '''Attempt to fill in any missing fields for a post.'''
    is_sub = isinstance(post, SubmissionPost)
    if post.datetime is None:
        try:
            praw_inst = Submission if is_sub else Comment
            praw_sc = praw_inst(reddit=praw, id=post.pid)
            post.datetime = utc_to_dt(praw_sc.created_utc)
        except praw.exceptions.ClientException:
            pass
    if is_sub and post.title is None:
        try:
            praw_sub = Submission(reddit=praw, id=post.pid)
            post.title = praw_sub.title
        except praw.exceptions.ClientException:
            pass
    post.save()


def get_posts_by_ids(is_sub: bool, ids: List[str]) -> List[Dict]:
    base_url = "https://api.pushshift.io/reddit/"
    base_url += "submission" if is_sub else "comment"
    base_url += ("/search?ids=" + ",".join(ids))
    return requests.get(base_url).json()['data']


def fill_all_posts():

    # update posts without datetimes
    print('Comments:')
    print('\tRetrieving pids without datetimes .....')
    comment_subsets = Post.objects(datetime__exists=False).only('pid')
    pids = [c.pid for c in comment_subsets]

    print('\tRetrieving missing data from psaw .....')
    chunk_size = 1000
    n_chunks = int(len(pids) / chunk_size)
    for chunk_pids in np.array_split(pids, n_chunks):
        full_comments = get_posts_by_ids(False, chunk_pids)
    assert len(pids) == len(full_comments)

    print('\tUpdating comms with missing data .....')
    for pid, real_comm in tqdm(zip(pids, full_comments)):
        comment = Comment.objects(pid=pid).first()
        comment.datetime = utc_to_dt(real_comm.created_utc)
        comment.save()

    # update submissions without titles
    print('Submissions:')
    print('\tUpdating pids without titles/datetimes .....')
    sub_subsets = SubmissionPost.objects(Q(title__exists=False) | Q(datetime_exists=False))\
                                .only('pid')
    pids = [s.pid for s in sub_subsets]

    print('\tRetrieving missing data from psaw .....')
    n_chunks = int(len(pids) / chunk_size)
    for chunk_pids in np.array_split(pids, n_chunks):
        full_subs = get_posts_by_ids(True, chunk_pids)
    assert len(pids) == len(full_subs)

    print('\tUpdating subs with missing data .....')
    for pid, real_sub in tqdm(zip(pids, full_subs)):
        sub = Submission.objects(pid=pid).first()
        sub.datetime = utc_to_dt(sub.created_utc)
        sub.title = real_sub.title
        sub.save()


if __name__ == '__main__':
    connect_to_mongo()
    fill_all_posts()
