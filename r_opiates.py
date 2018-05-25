import csv
import time
import os
import praw
import datetime
import argparse
import filecmp
import glob
import shutil
import collections
from praw.models import MoreComments

###################################################################################################################################

# virtual environment "beautifulsoup"

###################################################################################################################################

# Argument routine 1
# Note you can access this information at the command line using 'help' -- type python r_opiates.py --help

parser = argparse.ArgumentParser()
parser.add_argument("--subreddit", default='opiates', help="what subreddit to scrape (default is opiates)")
parser.add_argument("--thread_folder_name", default='threads')
parser.add_argument("--comment_folder_name", default='comments')
parser.add_argument("--comment_working_folder_name", default='working')
parser.add_argument("--comment_duplicate_folder_name", default='duplicates')
parser.add_argument("--comment_master_folder_name", default='master')
parser.add_argument("--comment_complete_folder_name", default='complete')
parser.add_argument("--iterate_over_days", default='1')
parser.add_argument("--datestring_start", default=None, help="starting date string in correct format (example: January-19-2018). Default will set to be 7 days prior.")
parser.add_argument("--datestring_end", default=None, help="ending date string in correct format (example: January-19-2018). Default will be set to right now.")
parser.add_argument("--days_look_back", default='7')
parser.add_argument("--days_look_forward", default='0')
parser.add_argument("--client_id", default='2do-OPn-K3ii3A')
parser.add_argument("--client_secret", default='pBqEsDdGCk-E_n55vkb0ITFzl1Y')
parser.add_argument("--password", default='~KilbyAssignment')
parser.add_argument("--username", default='jrreimer')
parser.add_argument("--file_folder", default=None)
parser.add_argument("--look_back", default=1000)


args = parser.parse_args()

###################################################################################################################################

# Argument routine 2 - can alter the below and copy-paste into terminal python for real-time work


class ArgumentContainer(object):
    def __init__(self):
        self.subreddit = "opiates"
        self.thread_folder_name = "threads"
        self.comment_folder_name = "comments"
        self.comment_working_folder_name = 'working'
        self.comment_duplicate_folder_name = 'duplicates'
        self.comment_master_folder_name = 'master'
        self.comment_complete_folder_name = 'complete'
        self.iterate_over_days = '1'
        self.datestring_start = 'March-03-2018'
        self.datestring_end = 'March-05-2018'
        self.days_look_back = None
        self.days_look_forward = None
        self.client_id = '2do-OPn-K3ii3A'
        self.client_secret = 'pBqEsDdGCk-E_n55vkb0ITFzl1Y'
        self.password = '~KilbyAssignment'
        self.username = 'jrreimer'
        self.file_folder = None
        self.look_back = 1000

if 'args' not in dir():
    args = ArgumentContainer()

###################################################################################################################################


def main():

    print('---------------------------------------------------------------------------------')

    r = make_praw_agent(args)

    print('---------------------------------------------------------------------------------')

    start_time, end_time = generate_time_bands(args.datestring_start, args.datestring_end, args.days_look_back, args.days_look_forward)
    thread_folder, comment_folder, comment_working_folder, comment_master_folder, comment_duplicate_folder, comment_complete_folder = assign_working_dirs(args.thread_folder_name, args.comment_folder_name, args.comment_working_folder_name, args.comment_master_folder_name, args.comment_duplicate_folder_name, args.comment_complete_folder_name, args.subreddit, args.file_folder)

    print('---------------------------------------------------------------------------------')

    idlist, thread_filepath_csv = scrape_subreddit(r, args.iterate_over_days, start_time, end_time, args.subreddit, thread_folder, 1000)
    prefix_list = get_all_comments_from_idlist(r, idlist, comment_working_folder, args.subreddit, sep='-')

    print('---------------------------------------------------------------------------------')

    separate_unique_and_dup_files(comment_master_folder, comment_working_folder, comment_duplicate_folder, sep='-')

    print('---------------------------------------------------------------------------------')

    complete_comment_files(comment_master_folder, comment_complete_folder, prefix_list, sep='-')

    print('---------------------------------------------------------------------------------')

##################################################################################################################################


def generate_time_bands(datestring_start=None, datestring_end=None, days_look_back=None, days_look_forward=None):
    """
    Flexibly takes a variety of date/time arguments and turns into datetime band in local time
    """
    if datestring_start and datestring_end:
        start_time = time.mktime(time.strptime(datestring_start, '%B-%d-%Y'))
        end_time = time.mktime(time.strptime(datestring_end, '%B-%d-%Y')) + 24 * 60 * 60
    elif datestring_start and not datestring_end:
        start_time = time.mktime(time.strptime(datestring_start, '%B-%d-%Y'))
        end_time = start_time + int(days_look_forward) * 24 * 60 * 60
    elif datestring_end and not datestring_start:
        end_time = time.mktime(time.strptime(datestring_end, '%B-%d-%Y')) + 24 * 60 * 60
        start_time = end_time - int(days_look_back) * 24 * 60 * 60
    else:
        end_time = time.mktime(datetime.datetime.now().timetuple())
        start_time = end_time - int(days_look_back) * 24 * 60 * 60
    print('Capture start date and time: %s' % time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time)))
    print('Capture end date and time: %s' % time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time)))
    return int(start_time), int(end_time)


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


def make_praw_agent(args):
    user_agent = "scrape submissions to " + "/r/%s using praw/python/terminal. By /user/%s" % (args.subreddit, args.username)
    r = praw.Reddit(client_id=args.client_id,
                    client_secret=args.client_secret,
                    password=args.password,
                    username=args.username,
                    user_agent=user_agent)
    return r


def scrape_subreddit(r, iterate_over_days, start_time, end_time, subreddit, thread_folder, look_back=None):
    """
    scrape_subreddit pulls all top level threads between a given time frame
    outputs an idlist and a thread filepath
    """
    ids = []
    scrape_time = int(time.mktime(datetime.datetime.now().timetuple()))
    iterate_over = 86400 * int(iterate_over_days)
    thread_filepath_csv = os.path.join(thread_folder, "r_" + subreddit + "_" + str(start_time) + "_" + str(end_time) + '_' + 't' + str(scrape_time) + ".csv")
    print('Writing thread headers to file: %s' % thread_filepath_csv)
    i = 0
    with open(thread_filepath_csv, 'w') as f:
        writer = csv.writer(f)
        j = 0
        for submission in r.subreddit(subreddit).new(limit=1000):
            id = submission.id
            ids.append(id)
            a = (id, submission.url, submission.num_comments, submission.shortlink, submission.author, submission.title, submission.selftext, submission.created_utc)
            writer.writerow(a)
            i = i + 1
            j = j + 1
            if j > 5000:
                f.close()
                os.remove(thread_filepath_csv)
                raise Exception("Submissions greater than 5000.")
        time.sleep(2)
        print('%s threads from %s to %s' % (j, start_time, end_time))
        print('Total thread headers captured: %s' % i)
        idlist = list(set(ids))
        return idlist, thread_filepath_csv


def get_all_comments_from_idlist(r, idlist, comment_working_folder, subreddit, sep='-'):
    """
    Takes a set of threads ids and pulls comments from a set of threads and places them into a csv file
    """
    tot_downloads = len(idlist)
    k = 0
    for submission_id_string in idlist:
        k += 1
        print('%s/%s: %s' % (k, tot_downloads, submission_id_string))
        scrape_time = int(time.mktime(datetime.datetime.now().timetuple()))
        submission = r.submission(id=submission_id_string)
        subreddit_submission = subreddit + '_' + str(submission)
        filepath_csv = os.path.join(comment_working_folder, subreddit_submission + sep + str(scrape_time) + '.csv')
        check_for_more = 1
        while check_for_more is 1:
            num_mores = 0
            for comment in submission.comments:
                if isinstance(comment, MoreComments):
                    num_mores = num_mores + 1
            if num_mores is 0:
                check_for_more = 0
            else:
                submission.comments.get_comments(limit=None, threshold=0)
        with open(filepath_csv, 'w') as f:
            writer = csv.writer(f)
            submission.comments.replace_more(limit=0)
            for comment in submission.comments.list():
                a = (comment.id, submission.url, submission.num_comments, comment.parent_id, comment.body, comment.author, comment.created_utc)
                writer.writerow(a)
    full_file_list = glob.glob(os.path.join(comment_working_folder, '*'))
    return list(set([x.split(sep)[0].split('/')[-1] for x in full_file_list]))


def get_submission_idlist(thread_filepath_csv):
    """
    Opens a thread list csv file outputted by scrape_subreddit, and outputs a thread id list. Not necessary in main routine but
    Useful in case want to access something from disk
    """
    ids = []
    with open(thread_filepath_csv, 'r') as fp:
        reader = csv.reader(fp)
        for row in reader:
            ids.append(row[0])
    idlist = list(set(ids))
    return idlist


def separate_unique_and_dup_files(master_subfolder, working_subfolder, duplicate_subfolder, sep='-'):
    """Separates comment files that are complete duplicates, and puts them in a dups folder which can later be purged"""
    full_file_list = glob.glob(os.path.join(working_subfolder, '*'))
    prefix_list = list(set([x.split(sep)[0].split('/')[-1] for x in full_file_list]))
    move_to_dups = 0
    move_to_master_archive = 0
    total_count = len(prefix_list)
    j = 0
    new_count = 0
    dup_count = 0
    for prefix in prefix_list:
        j += 1
        new_master_list, discard_list = uniquify_prefix(prefix, master_subfolder, working_subfolder)
        new_count += len(new_master_list)
        dup_count += len(discard_list)
        print('uniquified prefix %s out of %s' % (j, total_count))
        for filename in new_master_list:
            shutil.move(filename, master_subfolder)
            move_to_master_archive += 1
            print('%s moved to master folder' % filename)
        for filename in discard_list:
            shutil.move(filename, duplicate_subfolder)
            move_to_dups += 1
            print('%s moved to discard folder' % filename)
    print("moving complete")
    # remaining_files = len(glob.glob(os.path.join(working_subfolder, '*')))
    print('Moved %s comment files to permanent archive; Moved %s comment files to duplicate archive' % (new_count, dup_count))


def uniquify_prefix(prefix, master_subfolder, working_subfolder):
    prefix_file_list_working = glob.glob(os.path.join(working_subfolder, '%s*' % prefix))
    prefix_file_list_master = glob.glob(os.path.join(master_subfolder, '%s*' % prefix))
    candidate_list, discard_list1 = return_nondup_files(prefix_file_list_working)
    new_master_list, discard_list2 = return_new_nondup_files(candidate_list, prefix_file_list_master)
    return new_master_list, discard_list1 + discard_list2


def return_new_nondup_files(candidate_list, master_list):
    new_master_list, discard_list = [], []
    for item in candidate_list:
        duplicate = False
        for master_item in master_list:
            if duplicate is False:
                if filecmp.cmp(master_item, item):
                    duplicate = True
        if duplicate:
            discard_list.append(item)
        else:
            new_master_list.append(item)
    return new_master_list, discard_list


def return_nondup_files(candidate_list):
    master_list, discard_list = [], []
    i = 0
    for item in candidate_list:
        if i == 0:
            master_list.append(item)
        else:
            duplicate = False
            for master_item in master_list:
                if filecmp.cmp(item, master_item):
                    duplicate = True
            if duplicate:
                discard_list.append(item)
            else:
                master_list.append(item)
        i += 1
    return master_list, discard_list


def complete_comment_files(master_subfolder, complete_comment_folder, prefix_list=None, sep='-'):
    '''compares multiple sets of master files'''
    if prefix_list is None:
        full_file_list = glob.glob(os.path.join(master_subfolder, '*'))
        prefix_list = list(set([x.split(sep)[0].split('/')[-1] for x in full_file_list]))
    lenpref = len(prefix_list)
    i = 0
    for prefix in prefix_list:
        i += 1
        file_list = glob.glob(os.path.join(master_subfolder, '%s*' % prefix))
        master_row_list = []
        for filepath_use in file_list:
            with open(filepath_use, 'r') as in_file:
                comment_file = csv.reader(in_file)
                for row in comment_file:
                    row2 = row[:2] + row[3:]
                    # print(row2)
                    if row2 not in master_row_list:
                        master_row_list.append(row2)
        print('completed %s out of %s prefixes' % (i, lenpref))
        outfilename = os.path.join(complete_comment_folder, '%s.csv' % prefix)
        with open(outfilename, 'w') as outfile:
            print(outfilename)
            writer = csv.writer(outfile)
            writer.writerows(master_row_list)


def list_comment_threads_with_multiple_downloads(pathname):
    files = glob.glob('%s*' % pathname)
    stubs = [x.split('-')[0] for x in files]
    return [item for item, count in collections.Counter(stubs).items() if count > 1]

def all_submissions(thread_folder):
    row_list = []
    full_file_list = glob.glob(os.path.join(thread_folder, '*'))
    for file in full_file_list:    
        with open(file, 'r') as f:
            print(file)
            reader = csv.reader(f)
            for row in reader:
                a = tuple(row)
                row_list.append(a)
    row_list = list(set(row_list))
    #print row_list
    with open("/Users/jackiereimer/Dropbox/drug_pricing_data/opiates/threads/all_dumps.csv", 'w') as f:
        writer = csv.writer(f)
        for row in row_list:
            a = list(row)
            writer.writerow(a)

def all_comments(comment_complete_folder):
    row_list = []
    full_comment_list = glob.glob(os.path.join(comment_complete_folder, '*'))
    for file in full_comment_list:
        with open(file, 'r') as f:
            print(file)
            reader = csv.reader(f)
            for row in reader:
                a = tuple(row)
                row_list.append(a)
    row_list = list(set(row_list))
    #print row_list
    with open("/Users/jackiereimer/Dropbox/drug_pricing_data/opiates/comments/all_comments.csv", 'w') as f:
        writer = csv.writer(f)
        for row in row_list:
            a = list(row)
            writer.writerow(a)

if __name__ == '__main__':
    main()

