import ast

import tqdm
from mongoengine.queryset.visitor import Q
from spacy.lang.en import English
from spacy.tokens import Doc

from src.schema import Post


def add_spacy_to_mongo(nlp: English) -> int:
    """Add spacy field to all posts in mongo."""
    batch_size = 100
    num_total_posts = 0
    gen_posts = lambda: Post.objects(Q(spacy__exists=False) & Q(text__exists=True))\
                            .limit(batch_size)
    posts = gen_posts()
    while len(posts) > 0:
        num_total_posts += len(posts)
        for post in tqdm.tqdm(posts):
            text = post.text
            if type(text) == str:
                post.spacy = nlp(text).to_bytes()
                post.save()
        posts = gen_posts()
    return num_total_posts


def bytes_to_spacy(data: bytes, nlp: English) -> Doc:
    """Convert bytes data to a spacy doc."""
    doc = Doc(nlp.vocab).from_bytes(data)
    return doc


def literal_bytes_to_spacy(data: str, nlp: English) -> Doc:
    """Convert a string literal containing spacy bytes data to a spacy doc."""
    return bytes_to_spacy(ast.literal_eval(data), nlp)
