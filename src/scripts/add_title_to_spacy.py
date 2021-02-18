import tqdm
from spacy.lang.en import English
import spacy

from src.utils import connect_to_mongo
from src.schema import Post, SubmissionPost


def add_title_to_spacy(nlp: English):
    post_subsets = SubmissionPost.objects.only('pid')
    for post_subset in tqdm.tqdm(post_subsets):
        post = SubmissionPost.objects(pid=post_subset.pid).first()
        text = '. '.join([post.title, post.text])
        if type(text) == str:
            post.spacy = nlp(text).to_bytes()
            post.save()


if __name__ == '__main__':
    connect_to_mongo()
    nlp = spacy.load("en_core_web_sm")
    add_title_to_spacy(nlp)
