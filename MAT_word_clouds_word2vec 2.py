
# coding: utf-8

# # Word2Vec and LDA word cloud

# Project: Naloxone NLP
# 
# Author: Jackson Reimer
# 
# Last Update: May 4th, 2019
# 
# Function: This script inputs reddit data from csv's, formats them into lists and processes them for Latent Dirichlet Allocation and Word2Vec. This script outputs two primary sets of word clouds for both naloxone and non-naloxone related conversations and five primary word associations for Word2Vec exploration

# # Set up
# ## I. Import Packages

# In[1]:

import csv
import re
import os
import nltk
import argparse
import itertools
import glob
import datetime
import gensim
from scipy.sparse import csr_matrix
from scipy.sparse import coo_matrix
import pandas as pd
import numpy as np
import matplotlib as plt
from collections import Counter, defaultdict, deque, OrderedDict
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.stem import PorterStemmer
import nltk
from itertools import chain
from gensim import corpora, models
from gensim.models import LsiModel
from gensim.models import Word2Vec
import sys
from pprint import pprint
from gensim.models.coherencemodel import CoherenceModel
sys.path.insert(0, '/Users/jackiereimer/Dropbox/Drug Pricing Project/code/reddit_preprocessing')
from reddit_preprocessing import reddit_preprocessing as rp
np.random.seed(2018)
nltk.download('wordnet')
pd.options.display.max_colwidth = 900
porter_stemmer = PorterStemmer()
import matplotlib.pyplot as plt


# ## II. Set up default folder locations 

# In[3]:

class ArgumentContainer(object):
    def __init__(self):
        self.data_folder = "opiates"
        self.keyterm_folder = "keyterm_lists"
        self.complete_threads_file = "use_data/threads/all_dumps.csv"
        self.complete_comments_file = "use_data/comments/all_comments.csv"
        self.stop_words = "stop_words"
        self.location_folder = "location"
        self.meth_folder = "methadone"
        self.sub_folder = "suboxone"
        self.nalt_folder = "naltrexone"
        self.narc_folder = "naloxone"
        self.mat_folder = "mat"
        self.unit_folder = "unit"
        self.currency_folder = "currency"
        self.output_folder = "output"
        self.file_folder = None


if 'args' not in dir():
    args = ArgumentContainer()


# ## III. Define Functions
#     a. Input dataframe and define a subset with a keyword regex
#     b. Input a list of strings and output the list tokenized at the word level
#     c. Remove stop words from a list of tokeinzed strings
#     d. Replace synonyms with common phrase within list of tokenized strings
#     e. Stem words within a list of tokenized strings
#     f. create a dictionary from a list

# In[ ]:

def convert_key_word_threads_to_df(df, search_for, regexp, case_sensitive=False):
    """
    """
    print('Number of strings searched: %s' % df.shape[0])
    print('Number of keywords searching for: %s' % len(search_for))
    dt_start = datetime.datetime.now()
    print('Starting time:', dt_start)
    if not case_sensitive:
        flag = re.I
    else:
        flag = False
    i = 0
    for keyword in search_for:
        i += 1
        print('Word %s out of %s' % (i, len(search_for)))
        print('Time elapsed:', datetime.datetime.now() - dt_start)
        word = re.compile(regexp.format(keyword), flags=flag)
        df[keyword] = df.astype(str).sum(axis=1).str.contains(word, regex=True)
    return df

def tokenize_list_of_strings(list_of_strings):
    list_of_tokenized_strings = []
    i = 1
    for string in list_of_strings:
        print(round(i/len(list_of_strings), 5))
        tokenized_string = word_tokenize(string)
        list_of_tokenized_strings.append(tokenized_string)
        i += 1
    return list_of_tokenized_strings

def remove_stops_from_tokenized_list_of_strings(list_of_tokenized_strings, stop_words):
    list_of_stopped_strings = []
    i = 1
    for string in list_of_tokenized_strings:
        print(round(i/len(list_of_tokenized_strings), 5))
        filtered_string = [w for w in string if not w in stop_words]
        list_of_stopped_strings.append(filtered_string)
        i += 1
    return list_of_stopped_strings

def standardized_keywords(list_of_tokenized_strings, keyword_list, replace_with='narcan'):
	list_of_vetted_strings = []
	i = 1
	for word in keyword_list:
		j = 1
		for string in list_of_tokenized_strings:
			print("word %s: %s percent" % (i, round(j/len(list_of_tokenized_strings), 5)))
			filtered_string = [w.replace(word, replace_with) for w in string]
			list_of_vetted_strings.append(filtered_string)
			j += 1
		i += 1
	return list_of_vetted_strings

def stem_from_tokenized_list_of_strings(list_of_tokenized_strings):
    list_of_stemmed_strings = []
    i = 1
    for string in list_of_tokenized_strings:
        print(round(i/len(list_of_tokenized_strings), 5))
        stemmed_string = [porter_stemmer.stem(word) for word in string]
        list_of_stemmed_strings.append(stemmed_string)
        i += 1
    return list_of_stemmed_strings

def create_dict(items):
        return dict(enumerate(items, 1))


# 
# ## IV. Define keyword lists
# *Functions Defined in Previous file titled reddit pre-processing*
#     
#     a. Get filepaths based off of those listed in II.
#     b. Define non-case sensitive list of terms from Dropbox folder "keyword lists"
#     
# **NOTE 5/4:**
# 
# When multiple lists are being generated at once, the sequence in which Python reads the csv's changes between sessions but is the same within each session. Double check that the list is being defined properly before proceeding by changing the sequence in which the lists are defined below.

# In[8]:

locations_filepath, mat_filepath, all_comments_filepath, all_dumps_filepath, unit_filepath, currency_filepath, output_filepath, stopwords_filepath = rp.assign_location_dirs(args.data_folder, args.complete_threads_file, args.complete_comments_file, args.location_folder, args.mat_folder, args.unit_folder, args.currency_folder, args.output_folder, args.stop_words, args.file_folder)
state_init, locations = rp.generates_non_case_sensitive_list_of_keyterms(locations_filepath)
sub_words, meth_words, narc_words, nalt_words = rp.generates_non_case_sensitive_list_of_keyterms(mat_filepath)
currencies = rp.generates_non_case_sensitive_list_of_keyterms(currency_filepath)[0]
units = rp.generates_non_case_sensitive_list_of_keyterms(unit_filepath)[0]
more_stops = rp.generates_non_case_sensitive_list_of_keyterms(stopwords_filepath)[0]


#     a. Combine all MAT related words
#     b. Define Stopwords
#     c. Import all threads and comment and combine them into a list of posts

# In[10]:

mat_words = narc_words + meth_words + sub_words + nalt_words
stop = stopwords.words('english')
stop_all = stop + more_stops + ["'s", "'m", "'d", '...', ',', '.', '?', '!', '-', ':', "'ve", "'re", "''", "'"]

total_thread_tuples, total_threads = rp.list_of_threads_from_csv(args.data_folder, all_dumps_filepath)
total_comment_tuples, total_comments = rp.list_of_comments_from_csv(args.data_folder, all_comments_filepath)
total_posts = total_threads + total_comments


#     a. Define headers of features for data frames to be generated
#     b. Create comments, threads data frames and append the former to the latter
#     c. Convert all posts to lower case
#     d. Drop duplicates in sensitive features
#     e. Change missing text fields with empty text 
#     f. Append post body and post title

# In[11]:

thread_tuples_headers = ['post_id','time','no_comments', 'post_title', 'post_body']
comment_tuples_headers = ['comment_id', 'time', 'reply_id', 'post_body']

comment_df = pd.DataFrame(total_comment_tuples, columns=comment_tuples_headers)
thread_df = pd.DataFrame(total_thread_tuples, columns=thread_tuples_headers)
reddit_df = thread_df.append(comment_df)
reddit_df = reddit_df[['time', 'post_id', 'reply_id', 'no_comments', 'comment_id', 'post_title', 'post_body']]

reddit_df = reddit_df.applymap(lambda s:s.lower() if type(s) == str else s)

reddit_df = reddit_df.drop_duplicates(subset=['post_id', 'comment_id', 'post_title', 'post_body'])

reddit_df.post_title = reddit_df.post_title.fillna('')

reddit_df['full_post'] = reddit_df['post_body'] + " " + reddit_df['post_title']


# Define regular d:
# 
#     a. Matches standalone strings
#     b. Matches Numbers one and three digits in length
#     c. Matches standalone numbers of currency format with preceding currency symbol
#     d. Matches string of format 'digit(s)/letter(s)'
#     e. Matches the five words that surround the mention of a dollar sign
#     f. Requires three inputs (digit, keywords, digit), matches the digit number of words that surround keyword

# In[12]:

general_re = r"^.*\b({})\b.*$"
digit_re = r"\s\b\d{1,3}\b"
price_re = r'^.*[{}]\s?\d{{1,3}}(?:[.,]\d{{3}})*(?:[.,]\d{{1,2}})?.*$'
unit_price_re = r'[{}]?\d+[/]\D\S+'
surrounding_dollar_re = r'(?P<before>(?:\w+\W+){5})\$\d+(?:\.\d+)?(?P<after>(?:\W+\w+){5})'
surrounding_words_re = r'(?P<before>(?:\w+\W+){})[{}]\d+(?:\.\d+)?(?P<after>(?:\W+\w+){})' 


# Define Reddit as a dataframe with boolean features for each naloxone related keyword (Should take less than 10 minutes)

# In[13]:

reddit_df = convert_key_word_threads_to_df(reddit_df, narc_words, general_re, case_sensitive=False)


# Define a subset of `reddit_df` with no naloxone related feature with a True value

# In[16]:

narc_non_df = reddit_df.loc[(reddit_df['narcan'] != True) & (reddit_df['nrcn'] != True) & (reddit_df['narcn'] != True) & (reddit_df['evzio'] != True) & (reddit_df['naloxone'] != True) & (reddit_df['nar can'] != True) & (reddit_df['nrcan'] != True)]


# Define a subset of `reddit_df` with at least one naloxone related feature with a True value

# In[14]:

narc_only_df = reddit_df.loc[(reddit_df['narcan'] == True) | (reddit_df['nrcn'] == True) | (reddit_df['narcn'] == True) | (reddit_df['evzio'] == True) | (reddit_df['naloxone'] == True) | (reddit_df['nar can'] == True) | (reddit_df['nrcan'] == True)]


# **Note:** At this point all steps must be duplicated for `narc_non_df` and `narc_only_df`.
# 
# Convert dataframe column for full post to list.

# In[17]:

post_list = list(narc_non_df.full_post)


# Tokenize each post in the list. After each post it outputs the percent of list that has been tokenized

# In[18]:

post_list_tokenized = tokenize_list_of_strings(post_list)


# Remove stop words from the list of tokenized posts. After each post it outputs the percent of list that has been processed.

# In[19]:

post_list_no_stops = remove_stops_from_tokenized_list_of_strings(post_list_tokenized, stop_all)


# **Optional:** Remove all occurences of the keyword in each tokenized post

# In[20]:

post_list_no_keys = remove_stops_from_tokenized_list_of_strings(post_list_no_stops, narc_words)


# Stem each post using NLTK's Porter Stemmer

# In[21]:

post_stemmed = stem_from_tokenized_list_of_strings(post_list_no_stops)


# The first line creates a dictionary of each of the stemmed lists of tokens
# 
# The second line removes words from the dictionary that occur fewer than 15 times, in less than 90 percent of the texts. It then keeps the top 10,000 (from approximately 11,500) 

# In[23]:

dictionary = gensim.corpora.Dictionary(post_stemmed)
dictionary.filter_extremes(no_below=15, no_above=0.9, keep_n=100000)


# For each item in the list of stemmed, tokenized posts, create a compressed document-term matrix in the form of a list. Each item in the list signifies a post and the subsequent items are the frequencies of the words in the post. It has to be a compressed matrix because otherwise it would be incredibly sparse and impossible to keep in the working memory. At this point, the Bag-of-Words comes into play and the sequence of the words in the post is lost.

# In[24]:

bow_corpus = [dictionary.doc2bow(doc) for doc in post_stemmed]


# Apply TF-IDF weighting to the BoW corpus

# In[25]:

tfidf = models.TfidfModel(bow_corpus)


# In[26]:

corpus_tfidf = tfidf[bow_corpus]


# Apply LDA model to TF-IDF DTM. `num_topics=8` for `narc_non_df` and `num_topics=7` for `narc_only_df`

# In[ ]:

lda_model_tfidf = gensim.models.LdaMulticore(corpus_tfidf, num_topics=8, id2word=dictionary, passes=2, workers=2)


# This nexxt function is to generate the most distinctive post of each topic:
# 
# Create an empty dataframe
# for each topic in the model
# for each of the sorted rows in the 

# In[71]:

def format_topics_sentences(ldamodel=None, corpus=corpus_tfidf, texts=post_stemmed):
    # Init output
    sent_topics_df = pd.DataFrame()

    # Get main topic in each document
    for i, row_list in enumerate(ldamodel[corpus]):
        row = row_list[0] if ldamodel.per_word_topics else row_list            
        # print(row)
        row = sorted(row, key=lambda x: (x[1]), reverse=True)
        # Get the Dominant topic, Perc Contribution and Keywords for each document
        for j, (topic_num, prop_topic) in enumerate(row):
            if j == 0:  # => dominant topic
                wp = ldamodel.show_topic(topic_num)
                topic_keywords = ", ".join([word for word, prop in wp])
                sent_topics_df = sent_topics_df.append(pd.Series([int(topic_num), round(prop_topic,4), topic_keywords]), ignore_index=True)
            else:
                break
    sent_topics_df.columns = ['Dominant_Topic', 'Perc_Contribution', 'Topic_Keywords']

    # Add original text to the end of the output
    contents = pd.Series(texts)
    sent_topics_df = pd.concat([sent_topics_df, contents], axis=1)
    return(sent_topics_df)


df_topic_sents_keywords = format_topics_sentences(ldamodel=lda_model_tfidf, corpus=corpus_tfidf, texts=post_list)

# Format
df_dominant_topic = df_topic_sents_keywords.reset_index()
df_dominant_topic.columns = ['Document_No', 'Dominant_Topic', 'Topic_Perc_Contrib', 'Keywords', 'Text']
df_dominant_topic.head(10)


# In[72]:

pd.options.display.max_colwidth = 10000

sent_topics_sorteddf_mallet = pd.DataFrame()
sent_topics_outdf_grpd = df_topic_sents_keywords.groupby('Dominant_Topic')

for i, grp in sent_topics_outdf_grpd:
    sent_topics_sorteddf_mallet = pd.concat([sent_topics_sorteddf_mallet, 
                                             grp.sort_values(['Perc_Contribution'], ascending=False).head(1)], 
                                            axis=0)

# Reset Index    
sent_topics_sorteddf_mallet.reset_index(drop=True, inplace=True)

# Format
sent_topics_sorteddf_mallet.columns = ['Topic_Num', "Topic_Perc_Contrib", "Keywords", "Representative Text"]

# Show
sent_topics_sorteddf_mallet.head(10)


# In[75]:

doc_lens = [len(d) for d in df_dominant_topic.Text]

# Plot
plt.figure(figsize=(16,7), dpi=160)
plt.hist(doc_lens, bins = 1000, color='navy')
plt.text(750, 100, "Mean   : " + str(round(np.mean(doc_lens))))
plt.text(750,  90, "Median : " + str(round(np.median(doc_lens))))
plt.text(750,  80, "Stdev   : " + str(round(np.std(doc_lens))))
plt.text(750,  70, "1%ile    : " + str(round(np.quantile(doc_lens, q=0.01))))
plt.text(750,  60, "99%ile  : " + str(round(np.quantile(doc_lens, q=0.99))))

plt.gca().set(xlim=(0, 1000), ylabel='Number of Documents', xlabel='Document Word Count')
plt.tick_params(size=16)
plt.xticks(np.linspace(0,1000,9))
plt.show()
plt.savefig('lda_tfidf_gensim_narc_only_7_hist.png')


# In[ ]:

post_list = list(narc_non_df.full_post)
post_list_tokenized = tokenize_list_of_strings(post_list)
post_list_no_stops = remove_stops_from_tokenized_list_of_strings(post_list_tokenized, stop_all)
post_list_no_keys = remove_stops_from_tokenized_list_of_strings(post_list_no_stops, narc_words)
post_stemmed = stem_from_tokenized_list_of_strings(post_list_no_keys)


# In[77]:

dictionary = gensim.corpora.Dictionary(post_stemmed)
dictionary.filter_extremes(no_below=15, no_above=0.9, keep_n=100000)
bow_corpus = [dictionary.doc2bow(doc) for doc in post_stemmed]
tfidf = models.TfidfModel(bow_corpus)
corpus_tfidf = tfidf[bow_corpus]


# In[78]:

lda_model_tfidf = gensim.models.LdaMulticore(corpus_tfidf, num_topics=8, id2word=dictionary, passes=2, workers=2)


# In[ ]:

df_topic_sents_keywords = format_topics_sentences(ldamodel=lda_model_tfidf, corpus=corpus_tfidf, texts=post_list)

# Format
df_dominant_topic = df_topic_sents_keywords.reset_index()
df_dominant_topic.columns = ['Document_No', 'Dominant_Topic', 'Topic_Perc_Contrib', 'Keywords', 'Text']
df_dominant_topic.head(10)


# In[ ]:

df_topic_sents_keywords = format_topics_sentences(ldamodel=lda_model_tfidf, corpus=corpus_tfidf, texts=post_list)

# Format
df_dominant_topic = df_topic_sents_keywords.reset_index()
df_dominant_topic.columns = ['Document_No', 'Dominant_Topic', 'Topic_Perc_Contrib', 'Keywords', 'Text']
df_dominant_topic.head(10)


# In[80]:

pd.options.display.max_colwidth = 10000

sent_topics_sorteddf_mallet = pd.DataFrame()
sent_topics_outdf_grpd = df_topic_sents_keywords.groupby('Dominant_Topic')

for i, grp in sent_topics_outdf_grpd:
    sent_topics_sorteddf_mallet = pd.concat([sent_topics_sorteddf_mallet, 
                                             grp.sort_values(['Perc_Contribution'], ascending=False).head(1)], 
                                            axis=0)

# Reset Index    
sent_topics_sorteddf_mallet.reset_index(drop=True, inplace=True)

# Format
sent_topics_sorteddf_mallet.columns = ['Topic_Num', "Topic_Perc_Contrib", "Keywords", "Representative Text"]

# Show
sent_topics_sorteddf_mallet.head(10)


# In[81]:

doc_lens = [len(d) for d in df_dominant_topic.Text]

# Plot
plt.figure(figsize=(16,7), dpi=160)
plt.hist(doc_lens, bins = 1000, color='navy')
plt.text(750, 100, "Mean   : " + str(round(np.mean(doc_lens))))
plt.text(750,  90, "Median : " + str(round(np.median(doc_lens))))
plt.text(750,  80, "Stdev   : " + str(round(np.std(doc_lens))))
plt.text(750,  70, "1%ile    : " + str(round(np.quantile(doc_lens, q=0.01))))
plt.text(750,  60, "99%ile  : " + str(round(np.quantile(doc_lens, q=0.99))))

plt.gca().set(xlim=(0, 1000), ylabel='Number of Documents', xlabel='Document Word Count')
plt.tick_params(size=16)
plt.xticks(np.linspace(0,1000,9))
plt.show()
plt.savefig('lda_tfidf_gensim_narc_non_8_hist.png')


# In[49]:

reddit_model = Word2Vec(post_stemmed, size=1000, window=5, min_count=1, workers=4)


# In[ ]:



