import ast

import tqdm
from mongoengine.queryset.visitor import Q
from spacy.lang.en import English
from spacy.tokens import Doc

from src.schema import Post, SubmissionPost


def add_spacy_to_mongo(nlp: English) -> int:
    """Add spacy field to all posts in mongo."""
    post_subsets = Post.objects(Q(spacy__exists=False) & Q(text__exists=True))\
                       .limit(batch_size)\
                       .only('pid')

    for post_subset in tqdm.tqdm(post_subsets):
        post = Post.objects(pid=post_subset.pid).first()
        if isinstance(post, SubmissionPost):
            text = '. '.join([post.title, post.text])
        else:
            text = post.text
        if type(text) == str:
            post.spacy = nlp(text).to_bytes()
            post.save()

    return len(post_subsets)


def bytes_to_spacy(data: bytes, nlp: English) -> Doc:
    """Convert bytes data to a spacy doc."""
    doc = Doc(nlp.vocab).from_bytes(data)
    return doc


def literal_bytes_to_spacy(data: str, nlp: English) -> Doc:
    """Convert a string literal containing spacy bytes data to a spacy doc."""
    return bytes_to_spacy(ast.literal_eval(data), nlp)
