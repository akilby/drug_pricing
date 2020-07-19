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
            post.spacy = nlp(text).to_bytes()
    return len(posts)


def bytes_to_spacy(data: bytes, nlp: English) -> Doc:
    """Convert byte data to a spacy doc."""
    doc = Doc(nlp.vocab).from_bytes(data)
    return doc
