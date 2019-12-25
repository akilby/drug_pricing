import spacy
import numpy as np
import os
from nltk import Tree


def read_all_files(data_loc):
    """
    Reads text from each file in the given directory into a list of text

    String --> List[String]

    :param loc: the directory filepath containing the text files

    :returns all_text: the resulting text samples
    """
    all_text = []
    for fn in os.listdir(data_loc):
        fp = data_loc + "/" + fn
        with open(fp, "r") as myfile:
            all_text.append(myfile.read())
    return all_text


def get_freqs(doc, pos, prep_freqs):
    """
    Gets a frequency count by the given dependency relation

    :param doc: the parsed document
    :param pos: the part of speech to count
    :param prep_freqs: the frequency count to supplement

    :returns freq_counts: the frequency counts of each dependency relation
    """
    for tok in doc:
        if tok.pos_ == pos:
            key = tok.text
            if key not in prep_freqs.keys():
                prep_freqs[key] = 0
            else:
                prep_freqs[key] += 1
    return prep_freqs


def top_freqs(freqs, n):
    """
    Retrieves the keys for the top n values in the given frequency table

    Dict[String: Integer], Integer --> List[String]

    :param freqs: the frequency dictionary
    :param n: the number of top keys to retrieve

    :returns top_keys: the top n keys
    """
    top_keys = []
    top_num = []
    for k, v in freqs.items():
        if len(top_num) < 3:
            top_num.append(v)
            top_keys.append(k)
        else:
            if v > min(top_num):
                top_num[top_num.index(min(top_num))] = v
                top_keys[top_num.index(min(top_num))] = k
    return top_keys


def to_nltk_tree(node):
    # REFERENCED: https://stackoverflow.com/questions/36610179/how-to-get-the-dependency-tree-with-spacy
    if node.n_lefts + node.n_rights > 0:
        return Tree(node.orth_, [to_nltk_tree(child) for child in node.children])
    else:
        return node.orth_


def main():
    """
    Execute the program
    """

    # read in all text files
    print("Reading in data.....")
    root_dir = os.path.dirname(os.path.realpath(__file__))
    fn = "/work/akilby/drug_pricing_project/opiates/opiates/comments/all_comments.txt"
    with open(fn, "r") as myfile:
        all_text = myfile.read().split("\n")
    n_docs = 10  # subset number of samples for testing

    # instantiate spacy
    print("Loading spacy.....")
    nlp = spacy.load("en_core_web_sm")
    nlp.add_pipe(nlp.create_pipe('sentencizer'), first=True)

    # process the text with spacy in batches
    print("Iterating through text batches with spacy.....")
    i = 1  # for printing updates
    intervals = 10  # for printing updates
    cur_int = 1  # for printing updates
    sent_count = 0  # count of all sentences
    verb_count = 0  # count of all verbs
    ents = []  # all entities found in all batches
    prep_freqs = {}  # a map of prepositions to their count
    for doc in nlp.pipe(all_text):
        # for printing updates
        if i == len(all_text) * cur_int / intervals:
            print(f"\t {cur_int / intervals * 100}% of the way through.....")
            cur_int += 1
        i += 1

        # count the number of sentences in the doc
        sent_count += len(list(doc.sents))

        # calculate the average number of verbs per sentence in the doc
        verb_count += len([tok for tok in doc if tok.pos_ == "VERB"])

        # get frequency count for each prepositional lemma in the doc
        prep_freqs = get_freqs(doc, "ADP", prep_freqs)

        # get all entities in the doc
        ents += [e for e in doc.ents]

        # print out first n dependency treest
        if i < n_docs:
            for sent in doc.sents:
                print(sent.text)
                to_nltk_tree(sent.root).pretty_print()

    print("Getting final values.....")
    # get verb average
    verb_avg = verb_count / sent_count

    # get total prep counts
    total_preps = sum(prep_freqs.values())

    # get top three preps
    top_preps = top_freqs(prep_freqs, 3)

    # entity vals
    total_ents = len(ents)
    unique_ents = list(set([e.label_ for e in ents]))

    # product entities
    prod_ents = [e.text for e in ents if e.label_ == "PRODUCT"]

    # write answers to text file
    answers_fn = "analysis.txt"
    with open(root_dir + "/" + answers_fn, 'w') as myfile:
        myfile.write(f"Sentence count: {sent_count}\n\n")
        myfile.write(f"Average number of verbs per sentence: {verb_avg}\n\n")
        myfile.write(f"Total number of prepositions: {total_preps}\n\n")
        myfile.write(f"Top 3 prepositions: {top_preps}\n\n")
        myfile.write(f"Total number of entities: {total_ents}\n\n")
        myfile.write(f"Unique entities: {unique_ents}\n\n")
        myfile.write(f"Product entities: {prod_ents}")
    print("Done!")
    return


# initiate program if directly called
if __name__ == "__main__":
    main()
