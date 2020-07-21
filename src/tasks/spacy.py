import ast

import tqdm
from mongoengine.queryset.visitor import Q

from spacy.lang.en import English
from spacy.tokens import Doc
from src.schema import Post


def add_spacy_to_mongo(nlp: English) -> int:
    """Add spacy field to all posts in mongo."""
    posts = Post.objects(Q(spacy_exists=False) & Q(text_exists=True))
    for post in tqdm.tqdm(posts):
        text = post["text"]
        if type(text) == str:
            post.spacy = str(nlp(text).to_bytes())
    return len(posts)


def bytes_to_spacy(data: bytes, nlp: English) -> Doc:
    """Convert bytes data to a spacy doc."""
    doc = Doc(nlp.vocab).from_bytes(data)
    return doc


def literal_bytes_to_spacy(data: str, nlp: English) -> Doc:
    """Convert a string literal containing spacy bytes data to a spacy doc."""
    return bytes_to_spacy(ast.literal_eval(data), nlp)

