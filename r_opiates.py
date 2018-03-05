import csv
import time
import os
import praw
import datetime
import argparse
import filecmp
import glob
import shutil
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
        self.datestring_start = 'January-19-2018'
        self.datestring_end = 'January-19-2018'
        self.days_look_back = None
        self.days_look_forward = None
        self.client_id = '2do-OPn-K3ii3A'
        self.client_secret = 'pBqEsDdGCk-E_n55vkb0ITFzl1Y'
        self.password = '~KilbyAssignment'
        self.username = 'jrreimer'
        self.file_folder = None


if 'args' not in dir():
    args = ArgumentContainer() 

###################################################################################################################################


def main():

    print('---------------------------------------------------------------------------------')

    start_time, end_time = generate_time_bands(args.datestring_start, args.datestring_end, args.days_look_back, args.days_look_forward)
    thread_folder, comment_folder, comment_working_folder, comment_master_folder, comment_duplicate_folder, comment_complete_folder = assign_working_dirs(args.thread_folder_name, args.comment_folder_name, args.comment_working_folder_name, args.comment_master_folder_name, args.comment_duplicate_folder_name, args.comment_complete_folder_name, args.subreddit, args.file_folder)

    print('---------------------------------------------------------------------------------')

    r = make_praw_agent(args)
    idlist, thread_filepath_csv = scrape_subreddit(r, args.iterate_over_days, start_time, end_time, args.subreddit, thread_folder)

    print('---------------------------------------------------------------------------------')

    get_all_comments_from_idlist(r, idlist, comment_working_folder, args.subreddit)

    print('---------------------------------------------------------------------------------')

    separate_unique_and_dup_files(comment_master_folder, comment_working_folder, comment_duplicate_folder, sep='-')

    print('---------------------------------------------------------------------------------')

    complete_comment_files(comment_master_folder, comment_complete_folder, sep='-') 

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
        use_path = os.path.join(file_folder, "r_%s/" % subreddit)
    else:
        if os.getcwd().split('/')[2] == 'akilby':
            use_path = "/Users/akilby/Dropbox/Research/Data/%s/" % subreddit
        else:
            use_path = "/Users/jackiereimer/Dropbox/%s/" % subreddit
    thread_folder = os.path.join(use_path, thread_folder_name)
    comment_folder = os.path.join(use_path, comment_folder_name)
    comment_working_folder = os.path.join(comment_folder, comment_working_folder_name)
    comment_duplicate_folder = os.path.join(comment_folder,comment_duplicate_folder_name)
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


def scrape_subreddit(r, iterate_over_days, start_time, end_time, subreddit, thread_folder):
    """
    scrape_subreddit pulls all top level threads between a given time frame
    outputs an idlist and a thread filepath
    """
    ids = []
    iterate_over = 86400 * int(iterate_over_days)
    thread_filepath_csv = os.path.join(thread_folder, "r_" + subreddit + "_" + str(start_time) + "_" + str(end_time) + ".csv")
    print('Writing thread headers to file: %s' % thread_filepath_csv)
    i = 0
    with open(thread_filepath_csv, 'w') as f:
        writer = csv.writer(f)
        for iteration in range(start_time, end_time, iterate_over):
            i1 = iteration
            i2 = iteration + iterate_over
            timestampstring = "timestamp:" + str(i1) + ".." + str(i2)
            j = 0
            for submission in r.subreddit(subreddit).search(timestampstring, sort="new", syntax="cloudsearch", limit=None):
                id = submission.id
                ids.append(id)
                a = (id, submission.url, submission.num_comments, submission.shortlink, submission.author, submission.title, submission.selftext, submission.created_utc)
                writer.writerow(a)
                i = i + 1
                j = j + 1
                if j > 500:
                    f.close()
                    os.remove(thread_filepath_csv)
                    raise Exception("Submissions greater than 500.")
            time.sleep(2)
            print('%s threads from %s to %s' % (j, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(i1)), time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(i2))))
        print('Total thread headers captured: %s' % i)
        idlist = list(set(ids))
        return idlist, thread_filepath_csv


def get_all_comments_from_idlist(r, idlist, comment_working_folder, subreddit):
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
        filepath_csv = os.path.join(comment_working_folder, subreddit_submission + '-' + str(scrape_time) + '.csv')
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
    for prefix in prefix_list:
        j += 1
        new_master_list, discard_list = uniquify_prefix(prefix, master_subfolder, working_subfolder)
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
    remaining_files = len(glob.glob(os.path.join(working_subfolder, '*')))
    print('Moved %s comment files to permanent archive; Moved %s comment files to duplicate archive' % (move_to_master_archive, remaining_files))


def uniquify_prefix(prefix, master_subfolder, working_subfolder):
    prefix_file_list_working = glob.glob(os.path.join(working_subfolder, '%s*' % prefix))
    prefix_file_list_master = glob.glob(os.path.join(master_subfolder, '%s*' % prefix))
    discard_list = []
    new_master_list = []
    if prefix_file_list_master != []:
        for item in prefix_file_list_working:
            duplicate = False
            for master_item in prefix_file_list_master:
                if duplicate is False:
                    if filecmp.cmp(master_item, item):
                        duplicate = True
            if duplicate:
                discard_list.append(item)
            else:
                new_master_list.append(item)
    return new_master_list, discard_list


def complete_comment_files(master_subfolder, complete_comment_folder, sep='-'):
    '''compares multiple sets of master files'''
    full_file_list = glob.glob(os.path.join(master_subfolder, '*'))
    prefix_list = list(set([x.split(sep)[0].split('/')[-1] for x in full_file_list]))
    for prefix in prefix_list:
        file_list = glob.glob(os.path.join(master_subfolder, '%s*' % prefix))
        master_row_list = []
        for filepath_use in file_list:
            with open(filepath_use, 'r') as in_file:
                comment_file = csv.reader(in_file)
                for row in comment_file:
                    print(row)
                    if row not in master_row_list:
                        master_row_list.append(row)
        outfilename = os.path.join(complete_comment_folder, '%s.csv' % prefix)
        with open(outfilename, 'w') as outfile:
            print(outfilename)
            writer = csv.writer(outfile)
            writer.writerows(master_row_list)

if __name__ == '__main__':
    main()

