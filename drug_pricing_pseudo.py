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

adwords_filepath, mat_filepath, all_comments_filepath = assign_location_dirs()
locations, state_init = make_locations_list_from_adwords(adwords_filepath)
total_comment_list = all_comment_text(all_comments_filepath)
location_comments = find_keyword_comments(total_comment_list,locations, state_init)
positives = 

meth_words, sub_words, nalt_words, narc_words = make_mat_list(mat_filepath)
nalt_comments = find_keyword_comments(total_comment_list,nalt_words)


##################################################################################################################################

def assign_location_dirs():
    if os.getcwd().split('/')[2] == 'akilby':
        adwords_filepath = '/Users/akilby/Dropbox/Drug Pricing Project/locations/Locations.csv'
        mat_filepath = '/Users/akilby/Dropbox/Drug Pricing Project/mat_words/mat_words.csv'
        all_comments_filepath = '/Users/akilby/Dropbox/drug_pricing_data/opiates/comments/complete/all_comments.csv'
    else:
        adwords_filepath = '/Users/jackiereimer/Dropbox/Drug Pricing Project/locations/Locations.csv'
        mat_filepath = '/Users/jackiereimer/Dropbox/Drug Pricing Project/mat_words/mat_words.csv'
        all_comments_filepath = '/Users/jackiereimer/Dropbox/drug_pricing_data/opiates/comments/complete/all_comments.csv'
    return adwords_filepath, mat_filepath, all_comments_filepath


def make_locations_list_from_adwords(adwords_filepath):
    with open(adwords_filepath, encoding='utf8', errors='replace') as in_file:
        location_file = csv.reader(in_file)
        locations = list(location_file)
    state_init = [x[3] for x in locations]
    state_init = list(state_init)
    locations = [x[:2] for x in locations]
    locations = list(itertools.chain.from_iterable(locations))
    return locations, state_init


#locations = list(set(locations))

def make_mat_list(mat_filepath):
    with open(mat_filepath, 'r') as in_file:
        mat_file = csv.reader(in_file)
        mat = list(mat_file)
    meth_words = [x[0] for x in mat]
    sub_words = [x[1] for x in mat]
    nalt_words = [x[2] for x in mat]
    narc_words = [x[3] for x in mat]
    return meth_words, sub_words, nalt_words, narc_words


def all_comment_text(all_comments_filepath):
    total_comment = []
    with open(all_comments_filepath, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            total_comment.append(row[3])
    total_comment_list = list(set(total_comment))
    return total_comment_list

def find_keyword_comments(test_comments,test_keywords):
    keywords = '|'.join(test_keywords)
    word = re.compile(r"^.*\b({})\b.*$".format(keywords), re.I)
    newlist = filter(word.match, test_comments)
    final = list(newlist)
    return final

def find_keyword_comments(test_comments,test_keywords, test_keywords1):
    keywords = '|'.join(test_keywords)
    keywords1 = '|'.join(test_keywords1)
    word = re.compile(r"^.*\b({})\b.*$".format(keywords), re.I)
    word1 = re.compile(r"^.*\b({})\b.*$".format(keywords1))
    newlist = filter(word.match, test_comments)
    newlist1 = filter(word1.match, test_comments)
    final = list(newlist) + list(newlist1)
    return final

















def print_pos(location_comments):
    for c in location_comments:
        print(c, sep='\n')

def print_neg(total_comment_list, location_comments):
    for c in total_comment_list:
        if c not in location_comments:
            print(c, sep='\n')


def get_matches(test_comments, test_keywords, limit=3):
    keywords = '|'.join(test_keywords)
    word = re.compile(r"^.*\b({})\b.*$".format(test_keywords), re.I)
    final = []
    for comment in test_comments:
        results = process.extractBests(comment, locations, score_cutoff=85)
        final.append(results)
    final_list = list(final)
    return final_list




def find_keyword_comments(test_comments, test_keywords):
    return [[x, y] for x in test_keywords for y in test_comments if re.search(r'\b{}\b'.format(x.lower()), y.lower())]


def find_keyword_comments(test_comments,test_keywords):
   return [(word, [c for c in test_comments if re.findall(r'\b{}\b'.format(word), c, flags=re.I)]) for word in test_keywords]

test_keywords = ['Turin', 'Milan']
test_comments = ['This is a sent about turin.', 'This is a sent about manufacturing.', 'This is a sent about Milano.']
print(find_keyword_comments(list_of_sents, list_of_words))









def freq_dist(total_comment_list, mat_words, freq):
    raw_keyword_comments = find_keyword_comments(total_comment_list, mat_words)
    near_clean_mat_comments = remove_stopwords(raw_keyword_comments)
    final_most_common(near_clean_mat_comments, freq)

def remove_stopwords(raw_keyword_comments):
    stop_set = set(stopwords.words("english"))
    more_stops = ["i'm", "he's", "it's", "we're", "they're", "i've", "we've", "they've", "i'd", "he'd", "she'd", "we'd", "they'd", "i'll", "he'll", "she'll", "we'll", "they'll", "can't", "cannot", "let's", "that's", "who's", "what's", "here's", "there's", "when's", "where's", "why's", "how's", "daren't", "needn't", "oughtn't", "mightn't"]
    stop_set.union(more_stops)
    non_stopwords = []
    for comment in raw_keyword_comments:
        for word in word_tokenize(comment):
            if word not in stop_words:
                non_stopwords.append(word)
    return non_stopwords

def remove_mat_words(non_stopwords, mat_words):
    mat_words = set(mat_words)
    relevant_words = []
    for comment in non_stopwords:
        rw = []
        for word in comment:
            if word.lower() not in mat_words:
                rw.append(word)
            relevant_words.append(rw)
    return relevant_words


def final_most_common(non_stopwords, freq):
    rw = []
    for comment in non_stopwords:
        for word in word_tokenize(comment):
            rw.append(word)
    fdist = FreqDist(rw)
    print(fdist.most_common(freq))
































#''VERSION 1 of 3 DOES NOT WORK''' 
#ef identify_keyword_sentences(total_comment_list, locations):
#   with open(all_comments_filepath, 'r') as read_file:
#       reader = csv.reader(read_file)
#       unique_location_comments = []
#       for comment in reader:
#           location_comment = [word_tokenize(comment) for comment in total_comment_list if location in comment]
#           location_comment2 = location_comment[:2] + location_comment[3:]
#           if location_comment2 not in unique_location_comments:
#               unique_location_comments.append(location_comment)
#           print('added %s' % location_comment)
#   outfilename = '/Users/jackiereimer/Desktop/test2/test_narc.txt'
#   with open(outfilename, 'w') as outfile:
#       print(outfilename)
#       writer = csv.writer(outfile)
#       writer.writerows(unique_location_comments)


#''VERSION 2 of 3 DOES NOT WORK'''
#ef identify_keyword_comments(all_comments_filepath, total_comment_list):
#   ''' looks for terms within a set across all comments within a given filepath'''
#   narc_word = 'narcan'
#   narc_word2 = 'naloxone'
#   with open(all_comments_filepath, 'r') as read_file:
#       reader = csv.reader(read_file)
#       unique_keyterm_comments = []
#       for comment in reader:
#           keyterm_comment = [word_tokenize(comment) for comment in total_comment_list if narc_word in comment]
#           if keyterm_comment not in unique_keyterm_comments:
#               unique_keyterm_comments.append(keyterm_comment)
#               print('*' * 30)
#               print('added %s' % narc_word)
#               print('*' * 30)
#           keyterm2_comment = [word_tokenize(comment) for comment in total_comment_list if narc_word2 in comment]
#           if keyterm2_comment not in unique_keyterm_comments:
#               unique_keyterm_comments.append(keyterm2_comment)
#               print('*' * 30)
#               print('added %s' % narc_word2)
#               print('*' * 30)
#   return unique_keyterm_comments


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

#def find_locations(file, locations)
#    location_sentences = []
#    related_sentences = []
#    post_sentence_list = sent_tokenize(complete_file)
#        for sentence in post_sentence_list:
#            comment_word_list = word_tokenize(sentence) 
#            if location in comment_word_list:
#                return sentence
#                location_sentences.append(sentence)
#                try:
#                    return post_sentence_list[sentence-1]
#                    related_sentences.appen(sentence-1)
#                except IndexError as e:
#                    print('Top level post')
#                    pass
#                try:
#                    return post_sentence_list[sentence+1]
#                except IndexError as e:
#                    print('Last comment in post')
#                    pass
#

#def make_mat_list():
#    meth_words = ['methadone', 'mmt']
#    sub_words = ['suboxone', 'sub', 'suboxone', 'buprenex', 'butrans', 'probuphine', 'belbuca', 'bupe']
#    nalt_words = ['naltrexone' 'reviva', 'vivitrol', 'uldn']
#    narc_words = ['naloxone', 'narcan']
#    meth_words = [x.split(',') for x in meth_words]
#    sub_words = [x.split(',') for x in sub_words]
#    nalt_words = [x.split(',') for x in nalt_words]
#    narc_words = [x.split(',') for x in narc_words]
#    meth_words = list(itertools.chain.from_iterable(meth_words))
#    sub_words = list(itertools.chain.from_iterable(sub_words))
#    nalt_words = list(itertools.chain.from_iterable(nalt_words))
#    narc_words = list(itertools.chain.from_iterable(narc_words))
#    meth_words = list(set(meth_words))
#    sub_words = list(set(sub_words))
#    nalt_words = list(set(nalt_words))
#    narc_words = list(set(narc_words))
#    return meth_words, sub_words, nalt_words, narc_words


        #if keyword_comment not in unique_keyword_comments:
        #    unique_keyword_comments.append(keyword_comment)
        #    print('*' * 50)
        #    print('added %s' % keyword)
        #    print('*' * 50)



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
#from nltk import FreqDist
#import re
#
#raw = open('/Users/jackiereimer/Desktop/test2/opiates_7x1k1c-1518663169 copy.csv').read()
#fdist = nltk.FreqDist(wd.lower() for wd in raw if )
#
#
#
#f = open('/Users/jackiereimer/Desktop/test2/opiates_7x1k1c-1518663169 copy.csv')
#for line in f:
#    print(line.strip())
#
#raw = open('/Users/jackiereimer/Desktop/test2/opiates_7x1k1c-1518663169 copy.csv').read()
#type(raw)
#
#
#tokens = word_tokenize(raw)
#type(tokens)
#
#words = [w.lower() for w in tokens]
#type(words)
#
#vocab = sorted(set(words))
#type(vocab)
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


#def remove_stopwords(raw_keyword_comments):
#    stop_words = set(stopwords.words("english"))
#    non_stopwords = []
#    for comment in raw_keyword_comments:
#        nsw = []
#        for word in comment:
#            filtered_sentence = word.apply(lambda x : [w for w in x if w.lower() not in stop_words])
#            nsw.append(filtered_sentence)
#        non_stopwords.append(nsw)
#    return non_stopwords  
##    locations = [x[1:] + x[2].split(',') for x in locations]
#    locations = list(itertools.chain.from_iterable(locations))
 #   locations = list(set(locations))
#    for bad_char in bad_char_list:
#        locations = [x.replace(bad_char, '') for x in locations]
#    locations = [x for x in locations if x!='']
#    return locations
#