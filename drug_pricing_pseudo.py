import csv
import re
import os
import nltk
from nltk import FreqDist
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
import itertools

from fuzzywuzzy import process



##################################################################################################################################

adwords_filepath, mat_filepath, all_comments_filepath, all_dumps_filepath = assign_location_dirs()
locations, state_init = make_locations_list_from_adwords(adwords_filepath)
total_posts = all_text(all_comments_filepath, all_dumps_filepath)
location_posts = find_keyword_comments(total_posts,locations, state_init)
price_posts = find_price_posts(location_posts)

meth_words, sub_words, nalt_words, narc_words, mat_words = make_mat_list(mat_filepath)
nalt_comments = find_keyword_comments(total_comment_list,nalt_words)


##################################################################################################################################

def assign_location_dirs():
    if os.getcwd().split('/')[2] == 'akilby':
        adwords_filepath = '/Users/akilby/Dropbox/Drug Pricing Project/locations/Locations.csv'
        mat_filepath = '/Users/akilby/Dropbox/Drug Pricing Project/mat_words/mat_words.csv'
        all_comments_filepath = '/Users/akilby/Dropbox/drug_pricing_data/opiates/comments/complete/all_comments.csv'
        all_dumps_filepath = '/Users/akilby/Dropbox/drug_pricing_data/opiates/threads/all_dumps.csv'
    else:
        adwords_filepath = '/Users/jackiereimer/Dropbox/Drug Pricing Project/locations/Locations.csv'
        mat_filepath = '/Users/jackiereimer/Dropbox/Drug Pricing Project/mat_words/mat_words.csv'
        all_comments_filepath = '/Users/jackiereimer/Dropbox/drug_pricing_data/opiates/comments/complete/all_comments.csv'
        all_dumps_filepath = '/Users/jackiereimer/Dropbox/drug_pricing_data/opiates/threads/all_dumps.csv'
    return adwords_filepath, mat_filepath, all_comments_filepath, all_dumps_filepath


def make_locations_list_from_adwords(adwords_filepath):
    with open(adwords_filepath, encoding='utf8', errors='replace') as in_file:
        location_file = csv.reader(in_file)
        locations = list(location_file)
    state_init = [x[4] for x in locations]
    state_init = list(state_init)
    locations = [x[:3] for x in locations]
    locations = list(itertools.chain.from_iterable(locations))
    return locations, state_init

def make_mat_list(mat_filepath):
    with open(mat_filepath, 'r') as in_file:
        mat_file = csv.reader(in_file)
        mat = list(mat_file)
    meth_words = [x[0] for x in mat]
    sub_words = [x[1] for x in mat]
    nalt_words = [x[2] for x in mat]
    narc_words = [x[3] for x in mat]
    mat_words = [x[:3] for x in mat]
    mat_words = [item for sublist in mat_words for item in sublist]
    return meth_words, sub_words, nalt_words, narc_words, mat_words

def all_text(all_dumps_filepath, all_comments_filepath):
    total_text = []
    with open(all_comments_filepath, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            total_text.append(row[3])
    with open(all_dumps_filepath, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            total_text.append(row[5])
            total_text.append(row[6])
    total_posts = list(set(total_text))
    return total_posts

def find_keyword_comments(test_comments,test_keywords, test_keywords1):
    keywords = '|'.join(test_keywords)
    keywords1 = '|'.join(test_keywords1)
    word = re.compile(r"^.*\b({})\b.*$".format(keywords), re.I)
    word1 = re.compile(r"^.*\b({})\b.*$".format(keywords1))
    newlist = filter(word.match, test_comments)
    newlist1 = filter(word1.match, test_comments)
    final = list(newlist) + list(newlist1)
    return final

def find_price_posts(test_comments):
    word = re.compile(r'^.*[$\£\€]\s?\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{1,2})?.*$')
    newlist = filter(word.match, test_comments)
    final = list(newlist)
    return final

def find_number_posts(test_comments):
    word = re.compile(r'^.*\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{1,2})?.*$')
    newlist = filter(word.match, test_comments)
    final = list(newlist)
    return final

def print_pos(location_comments):
    for c in location_comments:
        print(c, sep='\n')

def print_neg(total_comment_list, location_comments):
    for c in total_comment_list:
        if c not in location_comments:
            print(c, sep='\n')


def fuzzy_try(test_comments, test_keywords):
    for comment in test_comments:
        list_results = []
        results = process.extractBests(comment, test_keywords, score_cutoff=90)
        list_results.append(results)
    return list_results


test_keywords = ['Turin', 'Milan']
test_comments = ['This is a sent about turin.', 'This is a sent about manufacturing.', 'This is a sent about Milano.']








##################################################################################################################################

meth_words, sub_words, nalt_words, narc_words, mat_words = make_mat_list(mat_filepath)
adwords_filepath, mat_filepath, all_comments_filepath, all_dumps_filepath = assign_location_dirs()
total_posts = all_text(all_comments_filepath, all_dumps_filepath)

nalt_comments = find_keyword_comments(total_comment_list,nalt_words)
freq_dist(total_comment_list, mat_words, freq)



##################################################################################################################################


def freq_dist(total_comment_list, mat_words, freq):
    raw_keyword_comments = find_keyword_comments(total_comment_list, mat_words)
    near_clean_mat_comments = stem_stop(raw_keyword_comments)
    clean_mat_comments = remove_mat_words(near_clean_mat_comments, mat_words)
    final_most_common(near_clean_mat_comments, freq)


def stem_stop(raw_keyword_comments):
    stop_set1 = list(stopwords.words("english"))
    porter = nltk.PorterStemmer()
    more_stops = ["i'm", "he's", "it's", "we're", "they're", "i've", "we've", "they've", "i'd", "he'd", "she'd", "we'd", "they'd", "i'll", "he'll", "she'll", "we'll", "they'll", "can't", "cannot", "let's", "that's", "who's", "what's", "here's", "there's", "when's", "where's", "why's", "how's", "daren't", "needn't", "oughtn't", "mightn't", '.', 'I', ',', "n't", ')', "'ve", '(', '...', 'It', "'re"]
    stop_set = stop_set1 + more_stops
    non_stopwords = []
    for comment in raw_keyword_comments:
        for word in word_tokenize(comment):
            if word not in stop_set:
                non_stopwords.append(porter.stem(word))
    return non_stopwords

def remove_mat_words(non_stopwords, test_keywords):
    relevant_words = []
    for word in non_stopwords:
        if word.lower() not in mat_words:
            relevant_words.append(word)
    return relevant_words


def final_most_common(non_stopwords, freq):
    rw = []
    for comment in non_stopwords:
        for word in word_tokenize(comment):
            rw.append(word)
    fdist = FreqDist(rw)
    print(fdist.most_common(freq))

def find_keyword_comments(test_comments,test_keywords):
    keywords = '|'.join(test_keywords)
    word = re.compile(r"^.*\b({})\b.*$".format(keywords), re.I)
    newlist = filter(word.match, test_comments)
    final = list(newlist)
    return final






locations = ['Turin', 'Milan']
state_init = ['NY', 'OK', 'CA']
sent = ['This is a sent about turin. ok?', 'This is a sent about milano.' 'Alan Turing was not from the state of OK.']
result = [('Turin', 'This is a sent about turin. ok?'), ('Milan', 'this is a sent about Melan'), ('OK', 'Alan Turing was not from the state of OK.')]
test_comments = ['This is a sentence with $10.00 in it.', 'This is a sentance with £1,000 in it.', 'This is a sentence with 1,100,000 in it.', 'This is a sentence with 10dollars in it.']









#def make_flat_comment_list(raw_keyword_comments):
#   flat_list = []
#   for comment in raw_keyword_comments:
#       for item in comment:
#           flat_list.append(item)
#   return flat_list

#    relevant_words = [word for word in str1 if not word in stop_words]
#        all_words.append(relevant_words)
#    all_words_set = set(all_words)
#    return all_words_set



#def identify_mat_sentences(total_comment_list, mat_terms):
#    mat_mention = []
#    for comment in total_comment_list:
#        total_sentences = sent_tokenize(comment)
#        for sentence in total_sentences:
#            total_words = word_tokenize(sentence)
#            for word in total_words:
#                if word in mat_terms:
#                    mat_mention.append(word)
#    mat_mentioned = set(mat_mention)
#    mat_sentences = [sent for sent in total_sentences if mat_mentioned in sent]
#    return location_sentences

#    location_mention = []
#    for comment in total_comment_list:
#        total_sentences = sent_tokenize(comment)
#        for sentence in total_sentences:
#            total_words = word_tokenize(sentence)
#            for word in total_words:
#                if word in locations:
#                    location_mention.append(word)
#    locations_mentioned = set(location_mention)
#    return locations_mentioned
#    location_comments = [comment for comment in total_sentences if locations_mentioned in sent]
#    return location_sentences

#relevant_words = []
#for review in raw_narc_comments:
#    rw=[]
#    for word in review:
#        if word not in stop_words:
#            rw.append(word)
#        relevant_words.append(rw)
#
#
#
#    total_sentence_list.append(total_sentences)
#    print(filttered_word_list)
#
#
#
#fdist = FreqDist(w for w in all_words_set if not w in mat_words or stop_words)
#
#
#
#
#def find_location_comments(test_comments, test_locations):
#    for comment in test_comments:
#        x = comment
#        for location in test_locations:
#            k = re.compile(r'\b%s\b' % location, re.I)
#            y = k.search(x)
#        print(y)
#            
#            y = k.search(x)


#def find_location_posts(total_comment_list, locations):
#    location_comments = []
#    for comment in total_comment_list:
#        if re.compile('|'.join(locations),re.IGNORECASE.search(comment)):
#            location_comments.append(comment)
#        else:
#            pass
#    return location_comments
#
#location_comments = (re.search(r'\b' + location + r'\b') in comment for location in test_locations)
#for comment in test_comments:
#    if (re.search(r'\b' + location + r'\b') in comment for location in test_locations):
#        print('%s' % location)
#
#
#
#    for location in locations:
#        location_comments = [comment for comment in total_comment_list if re.search(r'\b(%s)\b' % location, comment)]
#    return location_comments
#
#def find_location_posts(test_comments, locations):
#    for comment in test_comments:
#        used_locations = (re.search(r"\b"+ (location) + r"\b", comment) for location in locations)
#        print(used_locations)
#
#def find_location_posts(test_comments, locations):
#    for comment in test_comments:
#        used_locations = (re.search(r"\b(location)\b", comment) for location in locations)
#
#'|'.join(locations)+r"))",comment
#
#
#
#
#
#
#def find_location_posts(total_comment_list, locations):
#    for comment in total_comment_list:
#        print(re.findall(r"(?=("+'|'.join(locations)+r"))",comment))
#
#
#  
#prices = [w for w in comment if re.search('$[0_9]+', w)]
#    string_lst = ['fun', 'dum', 'sun', 'gum']
#    print(re.findall(r"(?=("+'|'.join(locations)+r"))",comment))
#
#    return prices
#

#        clean_comment = [word for word in raw_keyword_comments if word not in stop_words in comment]
#
#        nsw = []
#        for word in item:
#            if word.lower() not in stop_words:
#                nsw.append(word)
#            non_stopwords.append(nsw)
#    return non_stopwords
#
#
#        non_stopword = [word.lower() for word in raw_keyword_comments if not stop_words.lower() in word]
#        non_stopwords = non_stopwords + non_stopword
#    non_stopwords = list(set(non_stopwords))
#    return non_stopwords
#
#    for item in flat_keyword_comments:
#        nsw=[]
#        for word in item:
#            if word.lower() not in stop_words:
#                nsw.append(word)
#            non_stopwords.append(nsw)
#    return non_stopwords