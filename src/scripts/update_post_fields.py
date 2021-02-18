import tqdm

from src.utils import connect_to_mongo, get_praw, utc_to_dt
from src.schema import Post, SubmissionPost, CommentPost

from mongoengine.queryset.visitor import Q
from praw import Reddit
from praw.models import Submission, Comment


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


def fill_all_posts():
    # initialize parameters
    praw = get_praw()

    # update posts without datetimes
    print('Updating posts without datetimes .....')
    pids = Post.objects(datetime__exists=False).only('pid')
    for pid in tqdm.tqdm(pids):
        post = Post.objects(pid=pid).first()
        fill_missing_post_fields(post, praw)

    # update submissions without titles
    print('Updating submissions without titles .....')
    pids = SubmissionPost.objects(title__exists=False).only('pid')
    for pid in tqdm.tqdm(pids):
        post = Post.objects(pid=pid).first()
        fill_missing_post_fields(post, praw)


if __name__ == '__main__':
    connect_to_mongo()
    fill_all_posts()
