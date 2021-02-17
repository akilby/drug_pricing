import tqdm

from src.utils import connect_to_mongo, get_praw, utc_to_dt
from src.schema import Post, SubmissionPost, CommentPost

from mongoengine.queryset.visitor import Q
from praw import Reddit
from praw.models import Submission, Comment


def fill_missing_post_fields(post: Post, praw: Reddit):
    '''Attempt to fill in any missing fields for a post.'''
    if post.datetime is None:
        if isinstance(post, SubmissionPost):
            praw_subcomm = Submission(reddit=praw, id=post.pid)
        else:
            praw_subcomm = Comment(reddit=praw, id=post.pid)
        post.datetime = utc_to_dtc(praw_subcomm.created_utc)
    if isinstance(post, SubmissionPost) and post.title is None:
        praw_sub = Submission(reddit=praw, id=post.pid)
        post.title = praw_sub.title
    post.save()


def fill_all_posts():
    # initialize parameters
    praw = get_praw()
    batch_size = 100000

    # update posts without datetimes
    print('Updating posts without datetimes .....')
    gen_dt_posts = lambda: Post.objects(datetime__exists=False)\
                               .limit(batch_size)
    posts = gen_dt_posts()
    while len(posts) > 0:
        for p in tqdm.tqdm(posts):
            fill_missing_post_fields(p, praw)
        posts = gen_dt_posts()

    # update submissions without titles
    print('Updating submissions without titles .....')
    gen_title_subs = lambda: SubmissionPost.objects(title__exists=False)\
                                           .limit(batch_size)
    posts = gen_title_subs()
    while len(posts) > 0:
        for p in tqdm.tqdm(posts):
            fill_missing_post_fields(p, praw)
        posts = gen_title_subs()


if __name__ == '__main__':
    connect_to_mongo()
    fill_all_posts()
