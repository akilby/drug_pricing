import tqdm
from spacy.lang.en import English
import spacy

from src.utils import connect_to_mongo
from src.schema import Post, SubmissionPost


def add_title_to_spacy(nlp: English):
    pids = SubmissionPost.objects.only('pid')
    for pid in tqdm.tqdm(pids):
        post = SubmissionPost.objects(pid=pid).first()
        text = '. '.join([post.title, post.text])
        if type(text) == str:
            post.spacy = nlp(text).to_bytes()
            post.save()


if __name__ == '__main__':
    connect_to_mongo()
    nlp = spacy.load("en_core_web_sm")
    add_title_to_spacy(nlp)
