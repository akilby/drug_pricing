import csv
import re
import os
import glob
import nltk
from nltk import FreqDist
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
import itertools

tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')


'''Required for Each Analysis'''

# Establishes filepaths to relevant lists and data
adwords_filepath, mat_filepath, all_comments_filepath = assign_location_dirs()

# Establishes list of locations
locations = make_locations_list_from_adwords(adwords_filepath)

# Establishes lists of terms related to different treatments
meth_words, sub_words, nalt_words, narc_words, all_words = make_mat_list(mat_filepath)

# Takes all comments excel and creates list of tokenized lists
total_comment_list = all_comment_text(all_comments_filepath)


#Naltrexone Frequency Distribution: all comments at once
raw_narc_comments = identify_keyword_comments(total_comment_list, narc_words)
flat_narc_comments = make_flat_comment_list(raw_narc_comments)
near_clean_narc_comments = remove_stopwords(raw_narc_comments)
clean_narc_comments = remove_mat_words(near_clean_narc_comments, narc_words)
final_most_common(near_clean_narc_comments, 50)


def freq_dist(total_comment_list, mat_words, freq):
    raw_keyword_comments = identify_keyword_comments(total_comment_list, mat_words)
    near_clean_mat_comments = remove_stopwords(raw_keyword_comments)
    final_most_common(near_clean_mat_comments, freq)


#
raw_meth_comments = identify_keyword_comments(all_comments_filepath, total_comment_list, meth_words)
flat_meth_comments = make_flat_comment_list(raw_meth_comments)
near_clean_meth_comments = remove_stopwords(flat_meth_comments)
clean_meth_comments = remove_mat_words(near_clean_meth_comments, narc_words)
fifty_most_common(clean_meth_comments)


narc_sentences = identify_location_sentences(all_comments_filepath, total_comment_list, narc_words)


def assign_working_dirs(thread_folder_name, comment_folder_name, comment_working_folder_name, comment_master_folder_name, comment_duplicate_folder_name, comment_complete_folder_name, subreddit, file_folder=None):
    """
    Assigns working directories according to which computer being run on
    """
    if file_folder:
        use_path = os.path.join(file_folder, subreddit)
    else:
        if os.getcwd().split('/')[2] == 'akilby':
            use_path = "/Users/akilby/Dropbox/Research/Data/drug_pricing_data/%s/" % subreddit
        else:
            use_path = "/Users/jackiereimer/Dropbox/drug_pricing_data/%s/" % subreddit
    thread_folder = os.path.join(use_path, thread_folder_name)
    comment_folder = os.path.join(use_path, comment_folder_name)
    comment_working_folder = os.path.join(comment_folder, comment_working_folder_name)
    comment_duplicate_folder = os.path.join(comment_folder, comment_duplicate_folder_name)
    comment_master_folder = os.path.join(comment_folder, comment_master_folder_name)
    comment_complete_folder = os.path.join(comment_folder, comment_complete_folder_name)
    print('Thread folder: %s' % thread_folder)
    print('Comment folder: %s' % comment_folder)
    print('Master folder: %s' % comment_master_folder)
    print('Duplicate folder: %s' % comment_duplicate_folder)
    print('Working folder: %s' % comment_working_folder)
    print('Complete folder: %s' % comment_complete_folder)
    return thread_folder, comment_folder, comment_working_folder, comment_master_folder, comment_duplicate_folder, comment_complete_folder


def assign_location_dirs():
    if os.getcwd().split('/')[2] == 'akilby':
        adwords_filepath = '/Users/akilby/Dropbox/Drug Pricing Project/locations/AdWords.csv'
        mat_filepath = '/Users/akilby/Dropbox/Drug Pricing Project/mat_words/mat_words.csv'
        all_comments_filepath = '/Users/akilby/Dropbox/Research/Data/drug_pricing_data/opiates/comments/complete/all_comments.csv'
    else:
        adwords_filepath = '/Users/jackiereimer/Dropbox/Drug Pricing Project/locations/Adwords.csv'
        mat_filepath = '/Users/jackiereimer/Dropbox/Drug Pricing Project/mat_words/mat_words.csv'
        all_comments_filepath = '/Users/jackiereimer/Dropbox/drug_pricing_data/opiates/comments/complete/all_comments.csv'
    return adwords_filepath, mat_filepath, all_comments_filepath


def make_locations_list_from_adwords(adwords_filepath):
    with open(adwords_filepath, 'r') as in_file:
        location_file = csv.reader(in_file)
        locations = list(location_file)
    locations = [x[:5] for x in locations]
    locations = [x[1:] + x[2].split(',') for x in locations]
    locations = list(itertools.chain.from_iterable(locations))
    locations = list(set(locations))
    return locations

def make_mat_list(mat_filepath):
    with open(mat_filepath, 'r') as in_file:
        mat_file = csv.reader(in_file)
        mat = list(mat_file)
    meth_words = [x[0] for x in mat]
    sub_words = [x[1] for x in mat]
    nalt_words = [x[2] for x in mat]
    narc_words = [x[3] for x in mat]
    all_words = [x[4] for x in mat]
    return meth_words, sub_words, nalt_words, narc_words, all_words

def all_comment_text(all_comments_filepath):
    total_comment = []
    with open(all_comments_filepath, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            total_comment.append(row[3])
    total_comment_list = list(set(total_comment))
    return total_comment_list



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



'''VERSION 3 of 3 WORKS BUT PLACES DUPLICATE COMMENTS IN SET'''
def identify_keyword_comments(total_comment_list, keywords):
    keyword_comments = []
    for keyword in keywords:
        keyword_comment = [comment for comment in total_comment_list if keyword.lower() in comment]
        keyword_comments = keyword_comments + keyword_comment
        print('*' * 50)
        print('added %s' % len(keyword_comments))
        print('*' * 50)
    unique_keyword_comments = list(set(keyword_comments))
    return unique_keyword_comments

'''
[comment for comment in total_comment_list if 'narcan' in comment]
li1 = [comment for comment in total_comment_list if 'turin'.lower() in comment.lower()]
li2 = [comment for comment in li1 if "utring".lower() in [x.lower() for x in word_tokenize(comment)]]
'''
li1 = [comment for comment in total_comment_list if (r'turin\b') in comment.lower()]
li2 = [comment for comment in li1 if "utring".lower() in [x.lower() for x in word_tokenize(comment)]]




def remove_stopwords(raw_keyword_comments):
    stop_words = stopwords.words("english")
    non_stopwords = []
    for comment in raw_keyword_comments:
        for word in word_tokenize(comment):
            print(word)
            if word not in stop_words:
                non_stopwords.append(word)
    return non_stopwords
for comment in test_comments if re.search(r'\b(%s)\b')



def find_location_comments(total_comment_list,locations):
    location_comments = []
    for comment, location in [(comment,location) for comment in total_comment_list for location in locations]:
        word = re.compile(r'\b%s\b' % location, re.I)
        y = word.search(comment)
        print(y)
        if y != None:
            location_comments.append(y)
    print(location_comments)



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





def mat_fdist(all_words_set, mat_words)
    stop_words = set(stopwords.words("english"))
    for comment in total_comment_list:
    	fdist = FreqDist(w for w in all_words_set if not w in mat_words)
    print(fdist)

meth_words = ['methadone', 'mmt']
sub_words = ['suboxone', 'sub', 'suboxone', 'buprenex', 'butrans', 'probuphine', 'belbuca', 'bupe']
nalt_words = ['naltrexone' 'reviva', 'vivitrol', 'uldn']
narc_words = ['naloxone, narcan']



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


meth_words = ['methadone', 'mmt']
sub_words = ['suboxone', 'sub', 'suboxone', 'buprenex', 'butrans', 'probuphine', 'belbuca', 'bupe']
nalt_words = ['naltrexone' 'reviva', 'vivitrol', 'uldn']
narc_words = ['naloxone, narcan']

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
#
#