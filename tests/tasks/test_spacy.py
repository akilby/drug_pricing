"""Tests for adding spacy to mongo posts."""
from src.tasks.spacy import bytes_to_spacy
import spacy

nlp = spacy.load("en_core_web_sm")


def test_bytes_to_spacy():
    """Test that bytes spacy can properly be converted to spacy."""
    text = "sample text with words in it"
    doc = nlp(text)
    bdoc = doc.to_bytes()

    fresh_doc = bytes_to_spacy(bdoc, nlp)

    assert isinstance(fresh_doc, spacy.tokens.doc.Doc)
    assert fresh_doc.text == text
