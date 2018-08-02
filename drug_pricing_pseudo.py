import csv
import re
import os
import nltk
import argparse
import itertools
import glob
import datetime
from nltk import FreqDist
from collections import Counter, defaultdict, deque
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize, word_tokenize
from itertools import chain

##################################################################################################################################

# AEK notes:


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
# COMPLETE - Look to see if you can create a layered dictionary {'location':{price: (quantity: unit)}}
# COMPLETE - Look into a way that can take locations_dict, unit_dict and match by common comments. Then pair the values and convert the keys to integers

##################################################################################################################################


##################################################################################################################################
# Argument routine 1
# Note you can access this information at the command line using 'help' -- type python drug_pricing_pseudo.py --help

parser = argparse.ArgumentParser()
parser.add_argument("--data_folder", default='opiates', help="what folder of text csvs are being analyzed (default is opiates)")
parser.add_argument("--keyterm_folder", default='keyterm_lists', help="folder with csvs of keyterms")
parser.add_argument("--complete_threads_file", default='use_data/all_dumps.csv')
parser.add_argument("--complete_comments_file", default='use_data/all_comments.csv')
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
        self.complete_threads_file = "use_data/all_dumps.csv"
        self.complete_comments_file = "use_data/all_comments.csv"
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

def main():

    print('-' * 100)
    print('ASSIGNING DIRECTORIES AND KEYWORD LISTS')
    print('-' * 100)

    locations_filepath, mat_filepath, all_comments_filepath, all_dumps_filepath, unit_filepath, currency_filepath, output_filepath, stopwords_filepath = assign_location_dirs(args.data_folder, args.complete_threads_file, args.complete_comments_file, args.location_folder, args.mat_folder, args.unit_folder, args.currency_folder, args.output_folder, args.stop_words, args.file_folder)
    locations, state_init = generates_non_case_sensitive_list_of_keyterms(locations_filepath)
    meth_words, sub_words, nalt_words, narc_words = generates_non_case_sensitive_list_of_keyterms(mat_filepath)
    currencies = generates_non_case_sensitive_list_of_keyterms(currency_filepath)[0]
    units = generates_non_case_sensitive_list_of_keyterms(unit_filepath)[0]
    more_stops = generates_non_case_sensitive_list_of_keyterms(stopwords_filepath)[0]

    print('-' * 100)
    print('IMPORTING DATA INTO LISTS')
    print('-' * 100)

    total_thread_tuples, total_threads = list_of_threads_from_csv(args.data_folder, all_dumps_filepath)
    total_comment_tuples, total_comments = list_of_comments_from_csv(args.data_folder, all_comments_filepath)
    total_posts = total_threads + total_comments

    print('-' * 100)
    print('REGULAR EXPRESSIONS')
    print('-' * 100)

    general_re = r"^.*\b({})\b.*$" # matches standalone strings
    digit_re = r"\s\b\d{1,3}\b" # matches standalone numbers between 1 and 3 digits
    price_re = r'^.*[{}]\s?\d{{1,3}}(?:[.,]\d{{3}})*(?:[.,]\d{{1,2}})?.*$' # matches standalone numbers of currency format with preceding currency symbol
    unit_price_re = r'[{}]?\d+[/]\D\S+' # matches string of format 'digit(s)/letter(s)' (e.g. $40/gram, 5/mg)
    surrounding_dollar_re = r'(?P<before>(?:\w+\W+){5})\$\d+(?:\.\d+)?(?P<after>(?:\W+\w+){5})' # matches the five words that surround the mention of '$'
    surrounding_words_re = r'(?P<before>(?:\w+\W+){})[{}]\d+(?:\.\d+)?(?P<after>(?:\W+\w+){})' # requires three inputs (digit, keywords, digit), matches the digit number of words that surround keyword

    print('-' * 100)
    print('ORGANIZING DATA INTO DICTIONARIES FOR DPP')
    print('-' * 100)

    price_dict, processing_details = filter_strings_with_keywords1(total_posts, price_re, currencies, sep='', case_sensitive=False, search_for_chunksize=25)
    location_dict, processing_details = filter_strings_with_keywords(total_posts, general_re, locations, sep='|', case_sensitive=False, search_for_chunksize=25)
    state_init_dict, processing_details = filter_strings_with_keywords(total_posts, general_re, state_init, sep='|', case_sensitive=True, search_for_chunksize=25)
    unit_dict, processing_details = filter_strings_with_keywords(total_posts, general_re, units, sep='|', case_sensitive=False, search_for_chunksize=25)

    # ALTERNATIVELY READ IN PREXISTING DICTS USING FILENAME FORMAT ('_keywordlist_dict') 
    price_dict = readDict('_price_dict')
    location_dict = readDict('_location_dict')
    state_init_dict = readDict('_state_init_dict')
    unit_dict = readDict('_unit_dict')

    #
    writeDict(location_dict, '_location_dict')
    writeDict(price_dict, '_price_dict')
    writeDict(state_init_dict, '_state_init_dict')
    writeDict(unit_dict, '_unit_dict')
    writeDict(narc_dict, '_narc_dict')
    writeDict(meth_dict, '_meth_dict')
    writeDict(nalt_dict, '_nalt_dict')
    writeDict(sub_dict, '_sub_dict')

    print('-' * 100)
    print('MERGE AND EXPORT KEYWORD DICTIONARIES')
    print('-' * 100)

   location_unit_dict, filtered_location_unit_dict = merge_dictionaries_by_key(location_dict, unit_dict)
    writeDict(filtered_location_unit_dict, '_filtered_location_unit_dict')

    print('-' * 100)
    print('RETURN WORD FREQUENCIES SURROUNDING KEYWORDS')
    print('-' * 100)

    surrounding_dollar_frequency = surrounding_word_counter(location_dict, surrounding_dollar_re, more_stops)
    surrounding_word_frequency = surrounding_word_counter_robust(location_dict, currencies, 5, 5, sep='|', case_sensitive=False)
    write_list_of_tuples_to_csv(args.data_folder, args.currency_folder, surrounding_dollar_frequency, output_filepath)

    print('-' * 100)
    print('ORGANIZING DATA INTO DICTIONARIES FOR MAT')
    print('-' * 100)

    narc_dict, processing_details = filter_strings_with_keywords(total_posts, general_re, narc_words, sep='|', case_sensitive=False, search_for_chunksize=25)
    meth_dict, processing_details = filter_strings_with_keywords(total_posts, general_re, meth_words, sep='|', case_sensitive=False, search_for_chunksize=25)
    nalt_dict, processing_details = filter_strings_with_keywords(total_posts, general_re, nalt_words, sep='|', case_sensitive=False, search_for_chunksize=25)
    sub_dict, processing_details = filter_strings_with_keywords(total_posts, general_re, sub_words, sep='|', case_sensitive=False, search_for_chunksize=25)

    narc_dict = readDict('_narc_dict')
    meth_dict = readDict('_meth_dict')
    sub_dict = readDict('_sub_dict')
    nalt_dict = readDict('_nalt_dict')


    matched_posts = match_comment_and_thread_data(total_comment_tuples, total_thread_tuples)
    writeDict(matched_posts, '_opiates_matched_posts')
    readDict('_opiates_matched_posts')
    list_of_posts = combine_dict_values(matched_posts)
    clean_post_data_list = flatten_list_values(lists_of_posts)
    narc_dict, processing_details = filter_strings_with_keywords(clean_post_data_list, general_re, narc_words, sep='|', case_sensitive=False, search_for_chunksize=25)
    meth_dict, processing_details = filter_strings_with_keywords(clean_post_data_list, general_re, meth_words, sep='|', case_sensitive=False, search_for_chunksize=25)
    nalt_dict, processing_details = filter_strings_with_keywords(clean_post_data_list, general_re, nalt_words, sep='|', case_sensitive=False, search_for_chunksize=25)
    sub_dict, processing_details = filter_strings_with_keywords(clean_post_data_list, general_re, sub_words, sep='|', case_sensitive=False, search_for_chunksize=25)
    write_list_of_lists_to_csv(output_filepath, please)




    write_word_freq_to_csv(args.data_folder, args.currency_folder, price_dict, more_stops, output_filepath, freq=10000)
    location_posts, tuples_with_location_posts = filter_posts_for_keywords_from_lists(total_posts, locations, state_init)
    unit_dict, processing_details = filter_strings_with_keywords(total_posts, general_re, units, sep='|', case_sensitive=False, search_for_chunksize=25)
    filtered_unit_dict = filter_dict_values_for_keyword_and_number(unit_dict, general_re, unit_digit_re, locations)


##################################################################################################################################
# ASSIGNING DIRECTORIES AND KEYWORD LISTS

# TWO FUNCTIONS

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


##################################################################################################################################
# IMPORTING DATA INTO LISTS

# TWO FUNCTIONS

##################################################################################################################################

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
    """     
    total_text = []
    total_text_tuple = []
    with open(all_dumps_filepath, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            total_text_tuple.append((row[0], row[1], row[2], row[5], row[6]))
            total_text.append(row[5])
            total_text.append(row[6])
    total_post_tuples = list(set(total_text_tuple))
    total_posts = list(set(total_text))
    print('All r/%s/ threads aggregated' % data_folder)
    return total_post_tuples, total_posts

##################################################################################################################################
# FUNCTIONS THAT REGARD ANY FILTER AND SORT STRINGS INTO DICTIONARIES

# THREE FUNCTIONS

##################################################################################################################################
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
        keywords_grep = sep.join(chunk)
        word = re.compile(regexp.format(keywords_grep), flags=flag)
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

def filter_strings_with_keywords1(list_of_strings, regexp, search_for, sep='|', case_sensitive=False, search_for_chunksize=25):
    """
    THIS SHOULD NOT BE COPIED.
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

def find_digits_in_dict(dict1, regex):
    dict2 = defaultdict(list)
    word = re.compile(regex, flags=flag)
    for k, v in dict1.items():
        newlist = filter(word.match, k)
        if i.isdigit():
            dict2[i].append(v)
    return dict2


##################################################################################################################################
# MERGE AND EXPORT KEYWORD DICTIONARIES

# ONE FUNCTION
##################################################################################################################################

def merge_dicts_by_key(dict1, dict2):
    dict3 = defaultdict(list)
    dict4 = defaultdict(list)
    for k, v in chain(dict1.items(), dict2.items()):
        dict3[k].append(v)
    for k,v in dict3.items():
        if len(v) > 1:
            dict4[k].append(v)
        else:
            pass
    return dict3, dict4

##################################################################################################################################
# WRITING AND READING DICTIONARIES TO AND FROM CSV FILES

# TWO FUNCTIONS
##################################################################################################################################
def writeDict(dict, filename):
    filepath_use = os.path.join(output_filepath, filename + '.csv')
    with open(filepath_use, 'w') as f:
        w = csv.writer(f)
        for key, val in dict.items():
            w.writerow([key, val])

def readDict(filename):
    dict1 = {}
    filepath_use = os.path.join(output_filepath, filename + '.csv')
    with open(filepath_use, 'r') as f:
        r = csv.reader(f)
        for key, val in r:
            dict1[key] = val
    return dict1

##################################################################################################################################
# GENERATING WORD FREQUENCIES SURROUNDING KEYWORDS

# FOUR FUNCTIONS
##################################################################################################################################

def surrounding_word_counter(dict1, regexp, more_stops):
    list_of_strings = convert_dict_keys_to_list(dict1)
    flat_strings = ''.join(list_of_strings)
    rx = re.compile(regexp, re.I)
    words = [Counter(m.group('before').split() + m.group('after').split()) for m in rx.finditer(flat_strings)]
    summed_words = sum(words, Counter())
    sorted_words = summed_words.most_common()
    out_words = remove_stops_from_list_of_tuples(sorted_words, more_stops)
    return out_words


def surrounding_word_counter_robust(dict1, search_for, before_digit, after_digit, sep='|', case_sensitive=False):
    if not case_sensitive:
        flags=re.I
    else:
        flags=False
    list_of_strings = convert_dict_keys_to_list(dict1)
    flat_strings = ''.join(list_of_strings)
    keyword_grep = sep.join(search_for)
    regexp = r'(?P<before>(?:\w+\W+){})[{}]\d+(?:\.\d+)?(?P<after>(?:\W+\w+){})'
    formatted_regexp = regexp.format({before_digit}, keyword_grep, {after_digit})
    word = re.compile(formatted_regexp, flags=flags)
    words = [Counter(m.group('before').split() + m.group('after').split()) for m in word.finditer(flat_strings)]
    summed_words = sum(words, Counter())
    sorted_words = summed_words.most_common()
    return sorted_words


def convert_dict_keys_to_list(dict1):
    keys = []
    for item in list(dict1.keys()):
        keys.append(item)
    return keys

def convert_dict_values_to_list(dict1):
    values = []
    for item in list(dict1.values()):
        values.append(item)
    return values


def remove_stops_from_list_of_tuples(list_of_tuples ,more_stops):
    stop_set1 = list(stopwords.words("english"))
    stop_set = stop_set1 + more_stops
    stop_set = set(stop_set)
    out_tup = [i for i in list_of_tuples if i[0] not in stop_set]
    return out_tup

def write_list_of_tuples_to_csv(data_folder, keyword_folder, list_of_tuples, output_filepath):
    filepath_use = os.path.join(output_filepath, data_folder + '_' + keyword_folder + '_' + 'freqdist.csv')
    print('Printing to: %s' % filepath_use)
    with open(filepath_use, 'w') as f:
        writer = csv.writer(f)
        writer.writerows(list_of_tuples)
    print(filepath_use)



'''
REGULAR EXPRESSION EXAMPLE
'''
regexp = r'(?P<before>(?:\w+\W+){5})\$\d+(?:\.\d+)?(?P<after>(?:\W+\w+){5})'
rx = re.compile(regexp)
sentence = 'I have a sentence with $10.00 within it and this sentence is done. Here is another mention of $500 with no words after and there are more words.'
words = [Counter(m.group('before').split() + m.group('after').split()) for m in rx.finditer(sentence)]
summed_words = sum(words, Counter())
sorted_words = summed_words.most_common()
print(sorted_words)
'''
END EXAMPLE
'''
regexp = r'(?P<before>(?:\w+\W+){})[{}]\d+(?:\.\d+)?(?P<after>(?:\W+\w+){})'
keyword_grep = ''.join(currencies)
formatted_regexp = regexp.format({5}, keyword_grep, {5})
rx = re.compile(regexp)
sentence = 'I have a sentence with $10.00 within it and this sentence is done. Here is another mention of $500 with no words after and there are more words.'
words = [Counter(m.group('before').split() + m.group('after').split()) for m in rx.finditer(sentence)]
summed_words = sum(words, Counter())
sorted_words = summed_words.most_common()
print(sorted_words)




##################################################################################################################################
def counts_frequencies_of_five_words_before_after_price_mentions_from_list(dict1, regexp, search_for, more_stops, sep='|', case_sensitive=False):
    """
    Tuples of words and their frequency within a list of strings
    """
    if not case_sensitive:
        flag=re.I
    else:
        flag=False
    list_of_strings = convert_dict_keys_to_list(dict1)
    keyword_grep = sep.join(search_for)
    formatted_regexp = regexp.format(keyword_grep)
    word = re.compile(formatted_regexp, flag=flag)
    new_list_of_posts = filter(rx.match, list_of_posts)
    final_list_of_words = list(new_list_of_posts)
    flat_posts = ''.join(final_list_of_words)
    list_of_words = []
    for post in flat_posts:
        post.split()

    string1 = flat_posts.split()
    string2 =[s for s in string1 if s.isalpha() not in more_stops]
    final_list = [[x,string2.count(x)] for x in set(string2)]
    final_list.sort(key=lambda x: x[1])
    return final_list





##################################################################################################################################

#WORK IN PRORGRESS

##################################################################################################################################
regexp = r'(?P<before>(?:\w+\W+){5})\$\d+(?:\.\d+)?(?P<after>(?:\W+\w+){5})'
please1 = my_counter(location_dict, regexp)
surrounding_word_frequency = surrounding_word_counter_robust(location_dict, currencies, 5, 5, sep='', case_sensitive=False)


def write_word_freq_to_csv(data_folder, keyword_folder, dict1, more_stops, output_filepath, freq=10000):
    key_string_list = convert_dict_keys_to_list(dict1)
    surrounding_keyword_counter = []
    for string in key_string_list:
        surrounding_words = list(mycounter(string, search_for))
    print('Stop words removed, remaining stemmed and counted')
    filepath_use = os.path.join(output_filepath, data_folder + '_' + keyword_folder + '_' + 'freqdist.csv')
    print('Printing to: %s' % filepath_use)
    with open(filepath_use, 'w') as f:
        writer = csv.writer(f)
        writer.writerows(surrounding_words)

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




##################################################################################################################################


##################################################################################################################################




































































dict1 = convert_dict_keys_to_list(sub_dict)
filepath_use = os.path.join(output_filepath, '_total_threads.csv') 
with open(filepath_use, 'w') as f:
    writer = csv.writer(f)
    for item in total_threads:
        new_item = [item]
        writer.writerow(new_item)


value = [post for post in total_posts if meth_words not in word_tokenize(post)]
value1 = [post for post in value if narc_words not in word_tokenize(post)]
value2 = [post for post in value1 if sub_words not in word_tokenize(post)]
value3 = [post for post in value2 if nalt_words not in word_tokenize(post)]







list_of_strings = ['This is a sentence about Turin with 5 and $10.00 in it.', ' 2.5 Milan is a city with £1,000 in it.', 'Nevada $1,100,000.']
keywords = ['Turin', 'Milan' , 'Nevada']

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















def match_comment_and_thread_data(tuple_list1, tuple_list2, sep='/'):
    '''
    '''
    dict3 = defaultdict(list)
    dt_start = datetime.datetime.now()
    dict1 = dict([(a, (a, c, d, e)) for a, b, c, d, e in tuple_list2])
    print('(%s) first dictionary generated' % (datetime.datetime.now() - dt_start))
    not_words = ['https:']
    dict_refine = [(a.split('/'), b) for a,b in tuple_list1]
    dict_refine1 = [([word for word in words if len(word) == 6], key) for (words, key) in dict_refine]
    dict_refine2 = [([word for word in words if word not in not_words], key) for (words, key) in dict_refine1]
    dict_refine3 = [(''.join(a), b) for (a, b) in dict_refine2]
    dict2 = dict([(a, b) for a,b in dict_refine3])
    print('(%s) second dictionary generated' % (datetime.datetime.now() - dt_start))
    for k, v in chain(dict1.items(), dict2.items()):
        dict3[k].append(v)
    return dict3


def combine_dict_values(dict1):
    list_of_values = convert_dict_values_to_list(dict1)
    b = []
    i = 0
    for x in list_of_values:
        i += 1
        value = unpack(x)
        b.append(value)
        print(i, len(list_of_values))
    return b

def int_if_possible(val):
    try:
        return int(val)
    except ValueError:
        return val

def unpack(l):
    ret = [*l[0], *l[1:]]
    try:
    ret[0], ret[1] = int_if_possible(ret[1]), ret[0]
    return ret



def trans(lst):
    ret = [int(lst[0][1]), lst[0][0], lst[0][2], lst[0][3]]
    if len(lst) == 2:
        ret.append(lst[1])
    return ret

def flatten_list_values(list_of_strings):
    list_of_merged_lists = []
    for x in list_of_strings:
        x[2:] = [''.join(x[2:])]
        list_of_merged_lists.append(x)
    return list_of_merged_lists


def write_list_of_lists_to_csv(file_folder, list_of_lists):
    filepath_use = os.path.join(file_folder, 'opiates_paired_comments_posts.csv')
    print(filepath_use)
    with open(filepath_use, 'w') as f:
        w = csv.writer(f)
        for x in list_of_lists:
            w.writerow(x)





def write_dict_to_csv(output_filepath, dict1):
    filepath_use = os.path.join(output_filepath,'post_dictionary.csv')
    print(filepath_use)
    with open(filepath_use, 'w') as f:
        w = csv.writer(f)
        for key, value in dict1.items():
            w.writerow([key] + value)

def filter_lists_with_keyword_strings(list_of_lists, regexp, search_for, sep='|', case_sensitive=False):
    final_list_of_lists = []
    for x in list_of_lists:
        for a, b, c in x:
            final_item = [a, b, filter_strings_with_keywords(c, regexp, search_for, sep='|', case_sensitive=False, search_for_chunksize=25)]



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
        keywords_grep = sep.join(chunk)
        word = re.compile(regexp.format(keywords_grep), flags=flag)
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















def get_submission_idlist(thread_filepath_csv):
    """
    """
    ids = []
    with open(thread_filepath_csv, 'r') as fp:
        reader = csv.reader(fp)
        for row in reader:
            ids.append(row[0])
    idlist = list(set(ids))
    return idlist

prefix_list = list(set([x.split(sep)[0].split('.')[-1] for x in full_file_list]))
def separate_unique_and_dup_files(complete_subfolder, working_subfolder, duplicate_subfolder, sep='-'):
    """Separates comment files that are complete duplicates, and puts them in a dups folder which can later be purged"""
    full_file_list = glob.glob(os.path.join(complete_subfolder, '*'))
    prefix_list = set([x.split(sep)[0].split('_')[-1] for x in full_file_list])
    for file in full_file_list:
        with open(file, 'w') as f:

    for 



    dict3 = defaultdict(list)
    dt_start = datetime.datetime.now()
    dict1 = dict([(b, (a, c, d, e)) for a, b, c, d, e in tuple_list2])
    print('(%s) first dictionary generated' % (datetime.datetime.now() - dt_start))
    dict2 = dict([(a, b) for a,b in tuple_list1])
    print('(%s) second dictionary generated' % (datetime.datetime.now() - dt_start))
    for k, v in chain(dict1.items(), dict2.items()):
        dict3[k].append(v)
    return dict3













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