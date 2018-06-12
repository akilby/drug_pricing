import csv
import re
import os
import nltk
import argparse
import itertools
import glob
import datetime
from nltk import FreqDist
from collections import Counter, defaultdict
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize, word_tokenize


##################################################################################################################################

# AEK notes:
# COMPLETE - From running through hits, seems we should ignore ME and OR - much more likely to be words used with
# COMPLETE - emphasis rather than Maine and Oregon
# COMPLETE - State abbreviation list appears to contain duplicates, as does locations
# COMPLETE - DC, the District?
# COMPLETE - Chula vista

#JRR notes:
# COMPLETE 06.08 - Currency keyword list can't be read from csv
# OUTSTANDING - Dict concantonator innapropriately tokenizes comment strings
# OUTSTANDING - Change default csv program to TextWrangler from Excel
# COMPLETE 06.08 - Combine functions that generate tuples, lists of comments and threads
# COMPLETE 06.09 - TABLE: What are the words close to numbers/currencies 
                    # surface comments with numbers and currencies
                    # NLTK: Most common words in post (hopefully list of units emerge)
# OUTSTANDING - Take posts with units:
                    # derive subset with locations
                    # create a dictionary with
                    #{'jab': ('5','Chicago'), ('10','Atlanta')}, {'stamp':('unit','city')('unit','city')}
# OUTSTANDING - Package up the truly generalizable functions and start new document
# OUTSTANDING - Look to see if you can create a layered dictionary {'location':{price: (quantity: unit)}}


##################################################################################################################################


##################################################################################################################################
# Argument routine 1
# Note you can access this information at the command line using 'help' -- type python drug_pricing_pseudo.py --help

parser = argparse.ArgumentParser()
parser.add_argument("--data_folder", default='opiates', help="what folder of text csvs are being analyzed (default is opiates)")
parser.add_argument("--keyterm_folder", default='keyterm_lists', help="folder with csvs of keyterms")
parser.add_argument("--complete_threads_file", default='threads/all_dumps.csv')
parser.add_argument("--complete_comments_file", default='comments/complete/all_comments.csv')
parser.add_argument("--stop_words", default='stop_words')
parser.add_argument("--location_folder", default='location')
parser.add_argument("--mat_folder", default='mat')
parser.add_argument("--unit_folder", default='unit')
parser.add_argument("--currency_folder", default='currency')
parser.add_argument("--output_folder", default='output')
parser.add_argument("--file_folder", default=None)

args = parser.parse_args()


##################################################################################################################################

# Argument routine 2 - can alter the below and copy-paste into terminal python for real-time work

class ArgumentContainer(object):
    def __init__(self):
        self.data_folder = "opiates"
        self.keyterm_folder = "keyterm_lists"
        self.complete_threads_file = "threads/all_dumps.csv"
        self.complete_comments_file = "comments/complete/all_comments.csv"
        self.stop_words = "stop_words"
        self.location_folder = "location"
        self.mat_folder = "mat"
        self.unit_folder = "unit"
        self.currency_folder = "currency"
        self.output_folder = "output"
        self.file_folder = None


if 'args' not in dir():
    args = ArgumentContainer()

##################################################################################################################################
#  Functions included within main() ought to be run every time that the data needs to be worked with


def main():

    print('-' * 100)
    print('ASSIGNING DIRECTORIES AND KEYWORD LISTS')
    print('-' * 100)

    locations_filepath, mat_filepath, all_comments_filepath, all_dumps_filepath, unit_filepath, currency_filepath, output_filepath, stopwords_filepath = assign_location_dirs(args.data_folder, args.complete_threads_file, args.complete_comments_file, args.location_folder, args.mat_folder, args.unit_folder, args.currency_folder, args.output_folder, args.stop_words, args.file_folder)
    state_init, locations = generates_non_case_sensitive_list_of_keyterms(locations_filepath)
    meth_words, sub_words, nalt_words, narc_words = generates_non_case_sensitive_list_of_keyterms(mat_filepath)
    currencies = generates_non_case_sensitive_list_of_keyterms(currency_filepath)[0]
    units = generates_non_case_sensitive_list_of_keyterms(unit_filepath)[0]
    more_stops = generates_non_case_sensitive_list_of_keyterms(stopwords_filepath)[0]

# glob all keylists
# just pass name of keylist file

    print('-' * 100)
    print('PREPARING %s DATA' % data_folder)
    print('-' * 100)

    total_thread_tuples, total_threads = list_of_posts_from_csv(args.data_folder, all_dumps_filepath, content='threads')
    total_comment_tuples, total_comments = list_of_posts_from_csv(args.data_folder, all_comments_filepath, content='comments')
    total_posts = total_threads + total_comments

    print('-' * 100)
    print('-' * 100)


********************
total_post_dict = match_comment_and_thread_data(total_comment_tuples, total_thread_tuples)
file_folder = '/Users/jackiereimer/Desktop'
write_dict_to_csv(total_post_dict)

general_re = r"\b({})\b"
unit_digit_re = r"\s\b\d{1,3}\b"
price_re = r'^.*[{}]\s?\d{{1,3}}(?:[.,]\d{{3}})*(?:[.,]\d{{1,2}})?.*$'

price_dict, processing_details = filter_strings_with_keywords(total_comments, price_re, currencies, sep='', case_sensitive=False, search_for_chunksize=25)
write_word_freq_to_csv(args.data_folder, args.currency_folder, price_dict, more_stops, output_filepath, freq=10000)

location_dict, processing_details = filter_strings_with_keywords(total_comments, general_re, locations, sep='|', case_sensitive=True, search_for_chunksize=25)
location_posts, tuples_with_location_posts = filter_posts_for_keywords_from_lists(total_posts, locations, state_init)

narc_dict, processing_details = filter_strings_with_keywords(total_comments, general_re, narc_words, sep='|', case_sensitive=False, search_for_chunksize=25)
write_word_freq_to_csv(args.data_folder, args.mat_folder, narc_dict, more_stops, output_filepath, freq=10000)


unit_dict, processing_details = filter_strings_with_keywords(total_comments, general_re, units, sep='|', case_sensitive=False, search_for_chunksize=25)
filtered_unit_dict = filter_dict_values_for_keyword_and_number(unit_dict, general_re, unit_digit_re, locations, sep='|', case_sensitive=False)

price_posts = filter_posts_for_price_mentions_from_list(location_posts)
narc_comments = filter_posts_for_keywords_from_lists1(total_comment_list, keywords4)

##################################################################################################################################


def assign_location_dirs(data_folder, complete_threads_file, complete_comments_file, location_folder, mat_folder, unit_folder, currency_folder, output_folder, stopwords_folder, file_folder=None):
    """
    assigns directories according to computer program being run on
    """
    if file_folder:
        use_path = os.path.join(file_folder, data_folder)
    else:
        if os.getcwd().split('/')[2] == 'akilby':
            keywords_folder_filepath = '/Users/akilby/Dropbox/Drug Pricing Project/keyterm_lists'
            subreddit_filepath = '/Users/akilby/Dropbox/drug_pricing_data/%s/' % data_folder
        else:
            keywords_folder_filepath = '/Users/jackiereimer/Dropbox/Drug Pricing Project/keyterm_lists'
            subreddit_filepath = '/Users/jackiereimer/Dropbox/drug_pricing_data/%s/' % data_folder
    stopwords_filepath = os.path.join(keywords_folder_filepath, stopwords_folder)
    mat_filepath = os.path.join(keywords_folder_filepath, mat_folder)
    locations_filepath = os.path.join(keywords_folder_filepath, location_folder)
    unit_filepath = os.path.join(keywords_folder_filepath, unit_folder)
    currency_filepath = os.path.join(keywords_folder_filepath, currency_folder)
    output_filepath = os.path.join(keywords_folder_filepath, output_folder)
    all_comments_filepath = os.path.join(subreddit_filepath, complete_comments_file)
    all_dumps_filepath = os.path.join(subreddit_filepath, complete_threads_file)
    print('All thread file: %s' % all_dumps_filepath)
    print('All comment file: %s' % all_comments_filepath)
    print('Stop Words file: %s' % stopwords_filepath)
    print('Locations file: %s' % locations_filepath)
    print('MAT file: %s' % mat_filepath)
    print('Unit file: %s' % unit_filepath)
    print('Currency file: %s' % currency_filepath)
    print('Output folder: %s' % output_filepath)
    return locations_filepath, mat_filepath, all_comments_filepath, all_dumps_filepath, unit_filepath, currency_filepath, output_filepath, stopwords_filepath


def generates_non_case_sensitive_list_of_keyterms(keyword_filepath):
    """
    Reads a file containing lists of strings, outputs two lists of strings
    """
    full_file_list = list(set(glob.glob(os.path.join(keyword_filepath, '*'))))
    list_of_keyword_lists = []
    for file in full_file_list:
        print(file)
        with open(file, encoding='utf8', errors='replace') as in_file:
            keyword_file = csv.reader(in_file)
            keywords = list(keyword_file)
        keyword_list = list(set([x[0] for x in keywords]))
        list_of_keyword_lists.append(keyword_list)
    return list_of_keyword_lists

def list_of_posts_from_csv(data_folder, all_content_filepath, content='comments'):
    """
    Reads files containing all comments from data_folder
    outputs list of tuples with strings
    """
    total_text = []
    total_text_tuple = []
    with open(all_content_filepath, 'r') as f:
        reader = csv.reader(f)
        if content=='comments':
            for row in reader:
                total_text_tuple.append((row[1], row[3]))
                total_text.append(row[3])
        else:
            for row in reader:
                total_text_tuple.append((row[1], row[2], row[5], row[6]))
                total_text.append(row[5])
                total_text.append(row[6])
    total_post_tuples = list(set(total_text_tuple))
    total_posts = list(set(total_text)) 
    print('All r/%s/ %s aggregated' % (data_folder, content))
    return total_post_tuples, total_posts

def list_of_comments_from_csv(data_folder, all_comments_filepath):
    """
    Reads files containing all comments from data_folder
    outputs list of tuples with strings
    """
    total_text = []
    total_text_tuple = []
    with open(all_comments_filepath, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            total_text_tuple.append((row[1], row[3]))
            total_text.append(row[3])
    total_post_tuples = list(set(total_text_tuple))
    total_posts = list(set(total_text)) 
    print('All r/%s/ comments aggregated' % data_folder)
    return total_post_tuples, total_posts

def list_of_threads_from_csv(data_folder, all_dumps_filepath):
    """
    Reads file containing all threads from data_folder
    outputs list of tuples with four strings
    """     total_text_tuple.append((row[0], row[1], row[2], row[5], row[6]))
            total_text.append(row[5])
            total_text.append(row[6])
    total_post_tuples = list(set(total_text_tuple))
    total_posts = list(set(total_text))
    print('All r/%s/ threads aggregated' % data_folder)
    return total_post_tuples, total_posts

def filter_strings_with_keywords(list_of_strings, regexp, search_for, sep='|', case_sensitive=False, search_for_chunksize=25):
    """
    Filters list of strings that contain string from at least one of two lists of keywords
    """
    print('Number of strings searched: %s' % len(list_of_strings))
    print('Number of keywords searching for: %s' % len(search_for))
    dt_start = datetime.datetime.now()
    print('Starting time:', dt_start)
    if not case_sensitive:
        flag = re.I
    else:
        flag = False
    return_dict = {}
    i, total_chunks = 0, len([x for x in chunks(search_for, search_for_chunksize)])
    for chunk in chunks(search_for, search_for_chunksize):
        i += 1
        print('Chunk %s out of %s' % (i, total_chunks))
        print('Time elapsed:', datetime.datetime.now() - dt_start)
        formatted_regexp = regexp.format(sep.join(chunk))
        word = re.compile(formatted_regexp, flags=flag)
        newlist = filter(word.match, list_of_strings)
        filtered_with_matches = {y: set([x for x in search_for if re.compile(regexp.format(x), flags=flag).search(y)]) for y in newlist}
        return_dict = dict_update_append(return_dict, filtered_with_matches)
    print('%s posts found' % len(return_dict))
    print('Number of keywords found in strings:', len(set.union(*return_dict.values())))
    dt_end = datetime.datetime.now()
    print('Ending time:', dt_end)
    print('Time elapsed:', dt_end - dt_start)
    processing_details = (dt_end - dt_start, len(list_of_strings), len(search_for))
    return return_dict, processing_details


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def dict_update_append(dict1, dict2):
    return_dict = {}
    '''Updates a dictionary where the values are sets and need to be combined '''
    return_dict1 = {k: dict1[k] for k in dict1.keys() - dict2.keys()}
    return_dict2 = {k: dict2[k] for k in dict2.keys() - dict1.keys()}
    return_dict3 = {k: dict1[k] | dict2[k] for k in dict1.keys() & dict2.keys()}
    return_dict = {**return_dict, **return_dict1}
    return_dict = {**return_dict, **return_dict2}
    return_dict = {**return_dict, **return_dict3}
    return return_dict

def write_word_freq_to_csv(data_folder, keyword_folder, dict1, more_stops, output_filepath, freq=10000):
    key_string_list = listify_dict_values_for_nlp(dict1)
    list_of_stems, freq_dist = preprocess_list_of_strings_for_nlp(key_string_list, more_stops)
    print('Stop words removed, remaining stemmed and counted')
    filepath_use = os.path.join(output_filepath, data_folder + '_' + keyword_folder + '_' + 'freqdist.csv')
    print('Printing to: %s' % filepath_use)
    with open(filepath_use, 'w') as f:
        writer = csv.writer(f)
        writer.writerows(freq_dist)

def listify_dict_keys_for_nlp(dict1):
    values = []
    for item in list(dict1.keys()):
        values.append(item)
    return values

def preprocess_list_of_strings_for_nlp(list_of_strings, more_stops, freq=10000):
    stop_set1 = list(stopwords.words("english"))
    porter = nltk.PorterStemmer()
    stop_set = stop_set1 + more_stops
    stop_set = set(stop_set)
    words = []
    final_words = []
    for string in list_of_strings:
        words.append(word_tokenize(string))
    flat_words = list(itertools.chain.from_iterable(words))
    for word in flat_words:
        if word in stop_set:
            pass
        else:
            final_words.append(porter.stem(word))
    fdist = FreqDist(final_words)
    fdist_values = fdist.most_common(freq)
    return final_words, fdist_values



for values in list(dict1.keys()):
for d in dict1:
    filtered_dict = dict1.update((k, [(list(re.findall(word1, v)), list(re.findall(word, v)))]) for k, v in dict1.items())
#reorder_dict = {v:k for k, v in dict1.items()}
#filtered_dict = dict1.update(((list(re.findall(word1, k.split())), list(re.findall(word, k.split()))), v) for k, v in dict1.items())


def filter_dict_values_for_keyword_and_number(dict1, regexp, final_regexp, search_for):
    new_results = {tuple(i for i in a.split() if i.isdigit() or i in search_for):b for a, b in dict1.items()}
    return new_results

def filter_dict_values_for_keyword_and_number(dict1, regexp, final_regexp, search_for, sep='|', case_sensitive=False):
    filtered_dict = {}
    for y in search_for:
        new_dict_item = {y: set([i for i in a.split() if i.isdigit()])} for a,b in dict1.items()
        for a,b in list(dict1.items()):
            new_dict_item = {{y: set([i for i in a.split() if i.isdigit()])}:b}
            filtered_dict.append(new_dict_item)
    return filtered_dict

    filtered_with_matches = {{y: set([x for x in a.split() if x.isdigit()] for y in search_for)}:b for a, b in dict1.items()}
    #{y: set([x for x in search_for if re.compile(regexp.format(x), flags=flag).search(y)]) for y in newlist}
    return filtered_with_matches

def write_dict_to_csv(data_folder, dict1):
    values = []
    for item in list(dict1.items()):
        values.append(item)
    filepath_use = os.path.join(output_filepath, data_folder + '_unit_location_number.csv')
    with open(filepath_use, 'w') as f:
        writer = csv.writer(f)
        writer.writerows(values)















    #rekeyed_dict1 = {tuple(i for i in a.split() if i in search_for):b for a,b in dict1.items()}
    final_tuple = (a, b, tuple(i for i in a.split if i.isdigit())) for a,b in dict1.items()
    return final_tuple

    new_results = {tuple((list(set(i for i in a.split() if i.isdigit()))), (list(set(i for i in a.split() if i in search_for)))):b for a, b in dict1.items()}
    return new_results

    for k, v in dict1:
        filtered_dict = dict1.update(k, tuple(list(re.findall(word1, v.split)), list(re.findall(word, v))))
    return filtered_dict

    
    return filtered_dict



        for chunk in chunks(search_for, search_for_chunksize):
            i += 1
            print('Chunk %s out of %s' % (i, total_chunks))
            print('Time elapsed:', datetime.datetime.now() - dt_start)
            formatted_regexp = regexp.format(sep.join(chunk))
            word = re.compile(formatted_regexp, flags=flag)
            newlist = filter(word.match, values)
            filtered_with_matches = {y: set([x for x in search_for if re.compile(regexp.format(x), flags=flag).search(y)]) for y in newlist}
            return_dict = dict_update_append(return_dict, filtered_with_matches)
        print('%s posts found' % len(return_dict))
        print('Number of keywords found in strings:', len(set.union(*return_dict.values())))
        dt_end = datetime.datetime.now()
        print('Ending time:', dt_end)
        print('Time elapsed:', dt_end - dt_start)
        processing_details = (dt_end - dt_start, len(list_of_strings), len(search_for))
        return return_dict, processing_details






def filter_strings_with_keywords(list_of_strings, regexp, search_for, sep='|', case_sensitive=False, search_for_chunksize=25):
    """
    Filters list of strings that contain string from at least one of two lists of keywords
    """
    print('Number of strings searched: %s' % len(list_of_strings))
    print('Number of keywords searching for: %s' % len(search_for))
    dt_start = datetime.datetime.now()
    print('Starting time:', dt_start)
    if not case_sensitive:
        flag = re.I
    else:
        flag = False
    return_dict = {}
    i, total_chunks = 0, len([x for x in chunks(search_for, search_for_chunksize)])
    for chunk in chunks(search_for, search_for_chunksize):
        i += 1
        print('Chunk %s out of %s' % (i, total_chunks))
        print('Time elapsed:', datetime.datetime.now() - dt_start)
        formatted_regexp = regexp.format(sep.join(chunk))
        word = re.compile(formatted_regexp, flags=flag)
        newlist = filter(word.match, list_of_strings)
        filtered_with_matches = {y: set([x for x in search_for if re.compile(regexp.format(x), flags=flag).search(y)]) for y in newlist}
        return_dict = dict_update_append(return_dict, filtered_with_matches)
    print('%s posts found' % len(return_dict))
    print('Number of keywords found in strings:', len(set.union(*return_dict.values())))
    dt_end = datetime.datetime.now()
    print('Ending time:', dt_end)
    print('Time elapsed:', dt_end - dt_start)
    processing_details = (dt_end - dt_start, len(list_of_strings), len(search_for))
    return return_dict, processing_details


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def dict_update_append(dict1, dict2):
    return_dict = {}
    '''Updates a dictionary where the values are sets and need to be combined '''
    return_dict1 = {k: dict1[k] for k in dict1.keys() - dict2.keys()}
    return_dict2 = {k: dict2[k] for k in dict2.keys() - dict1.keys()}
    return_dict3 = {k: dict1[k] | dict2[k] for k in dict1.keys() & dict2.keys()}
    return_dict = {**return_dict, **return_dict1}
    return_dict = {**return_dict, **return_dict2}
    return_dict = {**return_dict, **return_dict3}
    return return_dict








































































def filter_posts_for_keywords_from_lists1(list_of_strings, keywords_a):
    """

    """
    list_of_tuples = []
    for string in list_of_strings:
        keywords = '|'.join(keywords_a)
        keyword_rx = re.findall(r"^\b({})\b$".format(keywords), string, re.I)
        price_rx = re.findall(r'^[\$\£\€]\s?\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{1,2})?$', string)
        number_rx1 = re.findall(r'\b\d[.]\d{1,2}\b', string)
        number_rx2 = re.findall(r'\s\d\s', string)
    tuples_with_keyword_post = [(keyword_rx, price_rx, number_rx1, number_rx2)]


def collects_keywords_prices_numbers_from_list_of_strings(tuples_with_keyword_posts):
    """
    Inputs tuple with the format ('keyword', 'comment with keyword') and generates a list of tuples with ('keyword', 'price', 'number')
        The first regex catches currency mentions in either US or international format
        The second regex catches any mention of a number with whitespace on either side
        Line 1 is a for loop by keyword and outputs: (keyword, filter_object(any digit or mention of price within all posts with keyword))
        Line 2 turns the above filter object into a list of filtered strings outputting only the tuple of interest: (keyword, 'digit', 'mention of price')
    Note: Final item should be a flat list
    """
    price_re = re.compile(r'^.*[\$\£\€]\s?\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{1,2})?.*$')
    number_re = re.compile(r'\b\d[.]\d{1,2}\b')
    new_list = [(keyword, [re.findall(price_re, post) for post in posts], [re.findall(number_re, post) for post in posts]) for keyword, posts in tuples_with_keyword_posts]
    final = [(keyword, list(filter(lambda x:re.findall(r'\d', x), currencies)), list(filter(lambda y:re.findall(r'\d', y), posts))) for keyword, currencies, posts in new_list]
    return final


def counts_frequencies_of_five_words_before_after_price_mentions_from_list(list_of_posts):
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


list_of_strings = ['This is a sentence about Turin with 5 and $10.00 in it.', ' 2.5 Milan is a city with £1,000 in it.', 'Nevada $1,100,000.']
keywords = ['Turin', 'Milan' , 'Nevada']

final_list = [('Turin', '$10.00', '5'), ('Milan', '£1,000', '2.5'), ('Nevada', '$1,100,000', '')]



##################################################################################################################################
'''FUNCTIONS RELATED TO MAT PROJECT'''

freq_dist_filepath = '/Users/jackiereimer/Dropbox/Drug Pricing Project/mat_words/'

def clean_and_generate_freq_distribution(total_posts, mat_words, freq, freq_dist_filepath):
    raw_keyword_comments = list_of_keywords_posts(total_posts, mat_words)
    near_clean_mat_comments = remove_stop_words_stem_remaining(raw_keyword_comments)
    clean_mat_comments = remove_mat_words(near_clean_mat_comments, mat_words)
    most_common_terms = final_most_common(clean_mat_comments, freq)
    filepath_use = os.path.join(freq_dist_filepath, mat_words[0] + '.csv')
    with open(filepath_use, 'w') as f:
        writer = csv.writer(f)
        for item in most_common_terms:
            writer.writerows(item)

    final_most_common(near_clean_mat_comments, freq)

def list_of_keywords_posts(list_of_strings, keywords_a):
    keywords = '|'.join(keywords_a)
    word = re.compile(r"^.*\b({})\b.*$".format(keywords), re.I)
    newlist = filter(word.match, list_of_strings)
    final = list(newlist)
    return final

def remove_stop_words_stem_remaining(raw_keyword_comments):
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
    stop_set1 = list(stopwords.words("english"))
    relevant_words = []
    for word in non_stopwords:
        if word.lower() not in keywords_a:
            relevant_words.append(word)
    return relevant_words

def final_most_common(non_stopwords, freq):
    rw = []
    rw_list = []
    for comment in non_stopwords:
        for word in word_tokenize(comment):
            rw.append(word)
    fdist = FreqDist(rw)
    rw_list.append(fdist.most_common(freq))
    return rw_list

def write_to_txt(list_of_posts):
    filepath_use = '/Users/jackiereimer/Desktop/%s.txt', % list_of_posts
    with open(list_of_posts, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(list_of_posts)

def filter_posts_for_keywords_from_lists(list_of_strings, keywords_a, keywords_b):
    """
    Filters list of strings that contain string from at least one of two lists of keywords
    keywords_a is not case sensitive, keywords_b is case sensitive
    """
    keywords = '|'.join(keywords_a)
    keywords1 = '|'.join(keywords_b)
    word = re.compile(r"^.*\b({})\b.*$".format(keywords), re.I)
    word1 = re.compile(r"^.*\b({})\b.*$".format(keywords1))
    newlist = filter(word.match, list_of_strings)
    newlist1 = filter(word1.match, list_of_strings)
    final = list(newlist) + list(newlist1)
    print('%s posts found' % len(final))
    tuples_with_keyword_post = [(i, [b for b in final if i in b]) for i in keywords_a]
    return final, tuples_with_keyword_post

def match_comment_and_thread_data(tuple_list1, tuple_list2):
    '''
    Functional Inneffective
    '''
    dt_start = datetime.datetime.now()
    dict1 = dict([(b, (a, c, d, e)) for a, b, c, d, e in tuple_list2])
    print('(%s) first dictionary generated' % (datetime.datetime.now() - dt_start))
    dict2 = dict([(a, b) for a,b in tuple_list1])
    print('(%s) second dictionary generated' % (datetime.datetime.now() - dt_start))
    dict3 = {}
    for key in set().union(dict1, dict2):
        if key in dict1: dict3.setdefault(key, []).extend(dict1[key])
        if key in dict2: dict3.setdefault(key, []).extend(dict2[key])
    return dict3

def match_comment_and_thread_data(tuple_list1, tuple_list2):
    '''
    Functional Mostly effective (tokenizes comments by character)
    '''
    dict3 = defaultdict(list)
    dt_start = datetime.datetime.now()
    dict1 = dict([(b, (a, c, d, e)) for a, b, c, d, e in tuple_list2])
    print('(%s) first dictionary generated' % (datetime.datetime.now() - dt_start))
    dict2 = dict([(a, b) for a,b in tuple_list1])
    print('(%s) second dictionary generated' % (datetime.datetime.now() - dt_start))
    dict3 = defaultdict(list)
    for key in set().union(dict1, dict2):
        for dic in [dict1, dict2]:
            if key in dic:
                dict3[key] += dic[key]
    return dict1, dict2, dict3


file_folder = '/Users/jackiereimer/Desktop'
def write_dict_to_csv(file_folder, dict1):
    filepath_use = os.path.join(file_folder,'post_dictionary.csv')
    print(filepath_use)
    with open(filepath_use, 'w') as f:
        w = csv.writer(f)
        for key, value in dict1.items():
            w.writerow([key] + value)

    full_post_tuples = []
    for x in tuple1:
        if x[0] in out_thread_dict.get[0]:
            combined_tuple = [x + out_thread_dict.get(x[1])]
            full_post_tuples.append(combined_tuple)
    return final_list

#final = [(keyword, list(filter(lambda x:re.findall(r'\d', x) and float(x) if x.isdigit() else True, currencies)), list(filter(lambda y:re.findall(r'\d', y) and float(y) if y.isdigit() else True, posts))) for keyword, currencies, posts in new_list]
#
#final = [(keyword, [list(filter(lambda x:re.findall(r'^.*[\$\£\€]\s?\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{1,2})?.*$', currency), currencies))], [list(filter(lambda x:re.findall(r'\b\d[.]\d{1,2}\b', x), posts))]) for keyword, posts in tuples_with_keyword_posts]
#
#
#def sort_key(d):
#  return (bool(re.findall(r'^\$[\d\.]+|\£[\d\.]+|\€[\d\.]+', d[0])), float(d[0][1:]) if not d[0][0].isdigit() else float(d[0])) if len(d) else (False, 0)
#
#beginning = [('keyword1', [], [], ['$5'], ['This has $6.50'], ['this has 4']), ('keyword2', [] ,[] ,[] ,[], [],['5.5'])]
#new_results = [(a, *sorted([re.findall(r'^\$[\d\.]+|\£[\d\.]+|\€[\d\.]+|[\d\.]+', i[0]) for i in b if i], key=sort_key)) for a, *b in map(lambda x:filter(None, x), beginning)]

#def counts_frequencies_of_words_before_after_price_mentions_from_list(list_of_strings):
#    """
#    Tuples of words and their frequency within a list of strings
#    """
#    flat_posts = ''.join(list_of_strings)
#    string1 = flat_posts.split()
#    string2 = [s for s in string1 if s.isalpha()]
#    final_list = [[x,string2.count(x)] for x in set(string2)]
#    final_list.sort(key=operator.itemgetter(1))
#    return final_list
#
#def list_of_keywords_posts(list_of_strings, keywords_a):
#    keywords = '|'.join(keywords_a)
#    word = re.compile(r"^.*\b({})\b.*$".format(keywords), re.I)
#    newlist = filter(word.match, list_of_strings)
#    final = list(newlist)
#    tuples_with_keyword_post = [(i, [b for b in final if i in b]) for i in keywords_a]
#    return final, tuples_with_keyword_post
#
#
#def filter_posts_for_price_mentions_from_list(list_of_strings):
#    """
#    List of strings that contain a $, €, £ and numbers in currency format within a list of strings
#    """
#    word = re.compile(r'^.*[\$\£\€]\s?\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{1,2})?.*$')
#    newlist = filter(word.match, list_of_strings)
#    final = list(newlist)
#    print('%s price posts found' % len(final))
#    return final
#
#
#def filter_posts_for_number_mentions_from_list(list_of_strings):
#    """
#    List of strings that contain a numbers in currecny format within a list of strings
#    """
#    word = re.compile(r'^.*\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{1,2})?.*$')
#    newlist = filter(word.match, list_of_strings)
#    final = list(newlist)
#    print('%s numbered posts found' % len(final))
#    return final

import sys
sys.path.append('Users/jackiereimer/filepath/to/folder_with_code')
