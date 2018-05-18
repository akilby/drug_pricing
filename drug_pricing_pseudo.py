import csv
import re
import os
import nltk
import argparse
import itertools
from nltk import FreqDist
from collections import Counter
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize, word_tokenize

##################################################################################################################################
# Argument routine 1
# Note you can access this information at the command line using 'help' -- type python drug_pricing_pseudo.py --help

parser = argparse.ArgumentParser()
parser.add_argument("--subreddit", default='opiates', help="what subreddit to scrape (default is opiates)")
parser.add_argument("--complete_threads_file", default='threads/all_dumps.csv')
parser.add_argument("--complete_comments_file", default='comments/complete/all_comments.csv')
parser.add_argument("--locations_file", default='locations/Locations.csv')
parser.add_argument("--mat_file", default='mat_words/mat_words.csv')
parser.add_argument("--unit_file", default='references/quantity_list')
parser.add_argument("--file_folder", default=None)

args = parser.parse_args()

##################################################################################################################################

# Argument routine 2 - can alter the below and copy-paste into terminal python for real-time work

class ArgumentContainer(object):
    def __init__(self):
        self.subreddit = "opiates"
        self.complete_threads_file = "threads/all_dumps.csv"
        self.complete_comments_file = "comments/complete/all_comments.csv"
        self.locations_file = "locations/Locations.csv"
        self.mat_file = "mat_words/mat_words.csv"
        self.unit_file = "references/quantity_list.csv"
        self.file_folder = None

if 'args' not in dir():
    args = ArgumentContainer()

##################################################################################################################################

def main():

    print('-' * 100)
    
    locations_filepath, mat_filepath, all_comments_filepath, all_dumps_filepath, unit_filepath = assign_location_dirs(subreddit, complete_threads_file, complete_comments_file, locations_file, mat_file, unit_file, file_folder=None)
    locations, state_init = make_locations_list_from_file(locations_filepath)
    meth_words, sub_words, nalt_words, narc_words, mat_words = make_mat_list(mat_filepath)
    units = make_unit_list(unit_filepath)
    
    print('-' * 100)
    
    total_posts = all_text(subreddit, all_dumps_filepath, all_comments_filepath)
    
    print('-' * 100)
    
    location_posts = find_keyword_comments(total_posts, locations, state_init)
    price_posts = find_price_posts(location_posts)
    
    print('-' * 1000)
    narc_comments = find_keyword_comments(total_comment_list,narc_words)

##################################################################################################################################

def assign_location_dirs(subreddit, complete_threads_file, complete_comments_file, locations_file, mat_file, unit_file, file_folder=None):
    """
    assigns directories according to computer program is being run on
    """
    if file_folder:
        use_path = os.path.join(file_folder, subreddit)
    else:   
        if os.getcwd().split('/')[2] == 'akilby':
            non_subreddit_filepath = '/Users/akilby/Dropbox/Drug Pricing Project/'
            subreddit_filepath = '/Users/akilby/Dropbox/drug_pricing_data/%s/' % subreddit
        else:
            non_subreddit_filepath = '/Users/jackiereimer/Dropbox/Drug Pricing Project/'
            subreddit_filepath = '/Users/jackiereimer/Dropbox/drug_pricing_data/%s/' % subreddit
    mat_filepath = os.path.join(non_subreddit_filepath, mat_file)
    locations_filepath = os.path.join(non_subreddit_filepath, locations_file)
    unit_filepath = os.path.join(non_subreddit_filepath, unit_file)
    all_comments_filepath = os.path.join(subreddit_filepath, complete_comments_file)
    all_dumps_filepath = os.path.join(subreddit_filepath, complete_threads_file)
    print('All thread file: %s' % all_dumps_filepath)
    print('All comment file: %s' % all_comments_filepath)
    print('Locations file: %s' % locations_filepath)
    print('MAT file: %s' % mat_filepath)
    print('Unit file: %s' % unit_filepath)
    return locations_filepath, mat_filepath, all_comments_filepath, all_dumps_filepath, unit_filepath


def make_locations_list_from_file(locations_filepath):
    """
    Reads a file containing lists of case sensitive and non-case sensitive strings
    """
    with open(locations_filepath, encoding='utf8', errors='replace') as in_file:
        location_file = csv.reader(in_file)
        locations = list(location_file)
    state_init = [x[4] for x in locations]
    state_init = list(state_init)
    locations = [x[:3] for x in locations]
    locations = list(itertools.chain.from_iterable(locations))
    print('Keyword lists complete')
    return locations, state_init

def make_mat_list(mat_filepath):
    """
    Reads a file containing lists of searchable strings
    """
    with open(mat_filepath, 'r') as in_file:
        mat_file = csv.reader(in_file)
        mat = list(mat_file)
    meth_words = [x[0] for x in mat]
    sub_words = [x[1] for x in mat]
    nalt_words = [x[2] for x in mat]
    narc_words = [x[3] for x in mat]
    mat_words = [x[:3] for x in mat]
    mat_words = [item for sublist in mat_words for item in sublist]
    print('Methadone Words: %s' % meth_words)
    print('Suboxone Words: %s' % sub_words)
    print('Naltrexone Words: %s' % nalt_words)
    print('Naloxone Words: %s' % narc_words)
    print('All MAT Words: %s' % mat_words)
    return meth_words, sub_words, nalt_words, narc_words, mat_words


def make_unit_list(unit_filepath):
    """
    Reads a file containing lists of searchable strings
    """
    with open(unit_filepath, 'r') as in_file:
        unit_file = csv.reader(in_file)
        unit = list(unit_file)
    units = [x[2:3] for x in unit]
    print('Unit list formed')
    return units

def all_text(subreddit, all_dumps_filepath, all_comments_filepath):
    """
    Reads files containing all comments and threads from subreddit 
    generates combined list of strings
    """
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
    print('All r/%s/ text aggregated' % subreddit)
    return total_posts

def find_keyword_comments(test_comments,keywords_a, keywords_b):
    """
    List of strings that contain keywords (both case sensitive and not) within list of strings
    """
    keywords = '|'.join(keywords_a)
    keywords1 = '|'.join(keywords_b)
    word = re.compile(r"^.*\b({})\b.*$".format(keywords), re.I)
    word1 = re.compile(r"^.*\b({})\b.*$".format(keywords1))
    newlist = filter(word.match, test_comments)
    newlist1 = filter(word1.match, test_comments)
    final = list(newlist) + list(newlist1)
    print('%s posts found' % len(final))
    return final

def find_price_posts(test_comments):
    """
    List of strings that contain a $, €, £ and numbers in currency format within a list of strings
    """
    word = re.compile(r'^.*[\$\£\€]\s?\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{1,2})?.*$')
    newlist = filter(word.match, test_comments)
    final = list(newlist)
    print('%s price posts found' % len(final))
    return final

def find_number_posts(test_comments):
    """
    List of strings that contain a numbers in currecny format within a list of strings 
    """
    word = re.compile(r'^.*\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{1,2})?.*$')
    newlist = filter(word.match, test_comments)
    final = list(newlist)
    print('%s numbered posts found' % len(final))
    return final

def find_surrounding_words(test_comments):
    """
    Tuples of words and their frequency within a list of strings
    """
    flat_posts = ''.join(test_comments)
    string1=flat_posts.split()
    string2=[s for s in string1 if s.isalpha()]
    final_list = [[x,string2.count(x)] for x in set(string2)]
    final_list.sort(key=operator.itemgetter(1))
    return final_list

def find_surrounding_words(list_of_posts):
    """
    Tuples of words and their frequency within a list of strings
    """
    stop_words = list(stopwords.words("english"))
    rx = re.compile(r'(?:\w+\W+){5}[\$\£\€]\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{1,2})?(?:\w+\W+){5}', re.I)
    new_list_of_posts = filter(rx.match, list_of_posts)
    final_list_of_words = list(new_list_of_posts) 
    flat_posts = ''.join(final_list_of_words)
    string1 = flat_posts.split()
    string2 =[s for s in string1 if s.isalpha() not in stop_words]
    final_list = [[x,string2.count(x)] for x in set(string2)]
    final_list.sort(key=lambda x: x[1])
    return final_list


def location_quantity_price(strings, keywords):
    grouped_strings = [(i, [b for b in strings if i in b]) for i in keywords]
    new_groups = [(a, filter(lambda x:re.findall(r'\d', x),[re.findall(r'[\$\d\£,]+', c) for c in b][0])) for a, b in grouped_strings]
    last_groups = [(a, list(filter(lambda x:re.findall(r'\d', x) and float(x) < 10 if x[0].isdigit() else True, b))) for a, b in new_groups]
    return last_groups


test_comments = ['This is a sentence about Turin with 5 and $10.00 in it.', ' 2.5 Milan is a city with £1,000 in it.', 'Nevada $1,100,000.']
keywords = ['Turin', 'Milan' , 'Nevada']

final_list = [('Turin', '$10.00', '5'), ('Milan', '£1,000', '2.5'), ('Nevada', '$1,100,000', '')]


import re
keywords = ['Turin', 'Milan' , 'Nevada']
strings = ['This is a sentence about Turin with 5 and $10.00 in it.', '2.5 Milan is a city with £1,000 in it.', 'Nevada and $1,100,000. and 10.09']
grouped_strings = [(i, [b for b in strings if i in b]) for i in keywords]
new_groups = [(a, filter(lambda x:re.findall('\d', x),[re.findall('[\$\d\.£,]+', c) for c in b][0])) for a, b in grouped_strings]
last_groups = [(a, list(filter(lambda x:re.findall('\d', x) and float(x) < 10 if x[0].isdigit() else True, b))) for a, b in new_groups]




##################################################################################################################################


def freq_dist(total_comment_list, mat_words, freq):
    raw_keyword_comments = find_keyword_comments1(total_comment_list, mat_words)
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

def remove_mat_words(non_stopwords, keywords_a):
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

def find_keyword_comments1(test_comments,keywords_a):
    keywords = '|'.join(keywords_a)
    word = re.compile(r"^.*\b({})\b.*$".format(keywords), re.I)
    newlist = filter(word.match, test_comments)
    final = list(newlist)
    return final

def nearestlist('string, flat_list', range(as in 5 for 5 on either side), freq):
    flat_price_posts = ''.join(price_posts)

    flat_price_posts = [item for sublist in price for item in sublist]
    return 20,000


#def print_pos(location_comments):
#    for c in location_comments:
#        print(c, sep='\n')
#
#def print_neg(total_comment_list, location_comments):
#    for c in total_comment_list:
#        if c not in location_comments:
#            print(c, sep='\n')

