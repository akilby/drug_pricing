from constants import SUB_COLNAMES, COMM_COLNAMES
from utils.functions import extract_csv
import os
from tqdm import tqdm
import functools as ft
import pandas as pd
import spacy
nlp = spacy.load("en_core_web_sm")

# build filepaths
CSV_DIR = "/work/akilby/drug_pricing_project/opiates/opiates/use_data"
comments_fp = os.path.join(CSV_DIR, "comments", "all_comments.csv")
threads_fp = os.path.join(CSV_DIR, "threads", "all_dumps.csv")

# read text into list
print("Reading text from csv files .....")
comm_text = extract_csv(comments_fp, COMM_COLNAMES)
sub_text = extract_csv(threads_fp, SUB_COLNAMES)
all_text = [p.text for p in (comm_text + sub_text) if type(p.text) == str]

# train spacy on all text blocks
print("Training spacy on text .....")
docs = []
for i in range(len(all_text)):
    docs.append(nlp(all_text[i]))

# extract location entities
print("Extracting location entities from spacy docs .....")
ents = ft.reduce(lambda acc, d: acc + list(d.ents), docs, [])
gpes = [e for e in ents if e.label_ == "GPE"]

# count entity frequencies
print("Counting unique locations .....")
gpe_counts = pd.Series(gpes).apply(lambda x: str(x)).value_counts()

# convert gpe counts to csv
print("Writing to csv .....")
gpe_counts.to_csv("./entity_counts.csv", header=False)
