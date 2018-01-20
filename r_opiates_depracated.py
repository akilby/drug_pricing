# COMPLETE DATA MANAGEMENT FUNCTIONS

# all_submissions collects all scraped posts into single csv

def all_submissions():
    row_list = []
    for dumpname in os.listdir("/Users/jackiereimer/Dropbox/r_opiates Data/threads/"):
        filepath_use = "/Users/jackiereimer/Dropbox/r_opiates Data/threads/" + dumpname
        if not dumpname.startswith('.'):
            with open(filepath_use, 'r') as f:
                reader = csv.reader(f)
                for row in reader:
                    a = tuple(row)
                    row_list.append(a)
    row_list = list(set(row_list))
    #print row_list
    with open("/Users/jackiereimer/Dropbox/r_opiates Data/threads/all_dumps.csv", 'w') as f:
        writer = csv.writer(f)
        for row in row_list:
            a = list(row)
            writer.writerow(a)


# all_comments collects all scraped comments into single csv

def all_comments():
    row_list = []
    for commentname in os.listdir("/Users/jackiereimer/Dropbox/r_opiates Data/comments/"):
#       thread_details = [commentname.partition("_")[0], commentname.partition("_")[2].partition(".")[0]]
        filepath_use = "/Users/jackiereimer/Dropbox/r_opiates Data/comments/" + commentname
        if not commentname.startswith('.'):
            with open(filepath_use, 'r') as f:
                reader = csv.reader(f)
                for row in reader:
#                   b = thread_details + row
#                   a = tuple(b)
                    a = tuple(row)
                    row_list.append(a)
    row_list = list(set(row_list))
    #print row_list
    with open("/Users/jackiereimer/Dropbox/r_opiates Data/comments/all_comments.csv", 'w') as f:
        writer = csv.writer(f)
        for row in row_list:
            a = list(row)
            writer.writerow(a)

#These three return all scraped data related to comments and threads containing a key term (case sensitive)

def submission_has_word(word):
    i = 0
    for line in open("/Users/jackiereimer/Dropbox/r_opiates Data/threads/all_dumps.csv"):
        if word in line:
            print(line)
            i = i + 1
    return i

def comment_has_word(word):
    i = 0
    for line in open("/Users/jackiereimer/Dropbox/r_opiates Data/comments/all_comments.csv"):
        if word in line:
            print(line)
            i = i + 1
    return i


def comment_has_words(word1,word2):
    i = 0
    for line in open("/Users/jackiereimer/Dropbox/r_opiates Data/comments/all_comments.csv"):
        if word1 in line:
            if word2 in line:
                print(line)
                i = i + 1
    return i

#These return all scraped data within a certain column. I thought these would be helpful
# for collecting ids to scrape comments but ultiately became somewhat useless


def get_usernames():
    usernames = []
    for comment_thread in os.listdir("/Users/jackiereimer/Dropbox/r_opiates Data/comments/"):
        filepath_use = "/Users/jackiereimer/Dropbox/r_opiates Data/comments/" + comment_thread
        if not comment_thread.startswith("."):
            with open(filepath_use, 'r') as fp:
                reader = csv.reader(fp)
                for row in reader:
                    usernames.append(row[3])
    for submission in os.listdir("/Users/jackiereimer/Dropbox/r_opiates Data/threads/"):
        filepath_use = "/Users/jackiereimer/Dropbox/r_opiates Data/threads/" + submission
        if not submission.startswith("."):
            with open(filepath_use, 'r') as fp:
                reader = csv.reader(fp)
                for row in reader:
                    usernames.append(row[4])
    usernames = list(set(usernames))
    return usernames

def get_submission_ids(subreddit):
    ids = []
    for dumpname in os.listdir("/Users/jackiereimer/Dropbox/r_opiates Data/threads/"):
        filepath_use = "/Users/jackiereimer/Dropbox/r_opiates Data/threads/" + dumpname
        if dumpname.startswith('r_'+ subreddit):
            with open(filepath_use, 'r') as fp:
                reader = csv.reader(fp)
                for row in reader:
                    ids.append(row[0])
    idlist = list(set(ids))
    return(idlist)


###################################################################################################################################

# WORK-IN-PROGRESS CODE


def comment_has_regexp(regexp):
    i = 0
    for line in open("/Users/jackiereimer/Dropbox/r_opiates Data/comments/all_comments.csv"):
        if re.search(regexp,line):
            print(line)
            i = i + 1
    return(i)


def new_and_old_submission_ids():
    all_ids = get_submission_ids(subreddit)
    ever_scraped_thread_ids = []
    for comment_thread in os.listdir("/Users/jackiereimer/Dropbox/r_opiates Data/comments/"):
        ever_scraped_thread_ids.append(comment_thread.partition("_")[0])
    ever_scraped_thread_ids = list(set(ever_scraped_thread_ids))
    unscraped_thread_ids = [x for x in all_ids if x not in ever_scraped_thread_ids]
    return(ever_scraped_thread_ids,unscraped_thread_ids)
#   print(ever_scraped_thread_ids,unscraped_thread_ids)

def update_comment_threads_to_fresh(time_difference):
    # time_difference is the threshold for the maximum time between when the thread was scraped, and the newest comment scraped, to see if it should be scraped again
    candidate_threads = new_and_old_submission_ids[0]


###################################################################################################################################
# PREVIOUS VERSIONS OF FINISHED CODE

def get_all_comments(submission_id_string,file_folder):
    print(submission_id_string)
    subreddit = r.subreddit('Opiates')
    submission = r.submission(id=submission_id_string)
    scrape_time = calendar.timegm(time.gmtime())
    subreddit ='Opiates_' + str(submission)
    filepath_csv = 'r_' + subreddit + '-' + str(scrape_time) + '.csv'
    check_for_more = 1
    while check_for_more is 1:
        num_mores = 0
        for comment in submission.comments:
            if isinstance(comment, MoreComments):
                num_mores = num_mores + 1
        if num_mores is 0:
            check_for_more = 0
        else :
            submission.comments.get_comments(limit=None, threshold=0)
    with open(filepath_csv, 'w') as f:
        writer = csv.writer(f)
        submission.comments.replace_more(limit=0)
        comment_queue = submission.comments[:]
        for comment in submission.comments.list():
            id = comment.id
            url = submission.url
            num_comments = submission.num_comments
            parent_id = comment.parent_id
            text = comment.body
            author = comment.author
            utc = comment.created_utc
            a=(id,url,num_comments,parent_id,text,author,utc)
            writer.writerow(a)

def scrape_all_comments(subreddit,file_folder):
    ids = []
    bad_id_list = []
    filepath_use = "/Users/jackiereimer/Dropbox/r_opiates Data/threads/r_opiates_1512950400_1513641600.csv"
    filepath_csv = os.path.join(file_folder,'r_' + subreddit + '-' + str(scrape_time) + '.csv')
    with open(filepath_use, 'r') as fp:
        reader = csv.reader(fp)
        for row in reader:
            ids.append(row[0])
    idlist = list(set(ids))
    subreddit = r.subreddit('Opiates')
    for id in idlist:
        bad_id = get_all_comments(id, file_folder)
        bad_id_list.append(bad_id)
    with open(filepath_csv, 'w') as f:
        writer = csv.writer(f)
        submission.comments.replace_more(limit=0)
        comment_queue = submission.comments[:]
        for comment in submission.comments.list():
            id = comment.id
            url = submission.url
            num_comments = submission.num_comments
            parent_id = comment.parent_id
            text = comment.body
            author = comment.author
            utc = comment.created_utc
            a=(id,url,num_comments,parent_id,text,author,utc)
            writer.writerow(a)
    return bad_id_list

def build_id_list(subreddit,file_folder)
    ids = []
    bad_id_list = []
    scrape_time = calendar.timegm(time.gmtime())
    filepath_use = os.path.join(file_folder,'r_' + subreddit + '_1513036800_1513728000.csv')
    filepath_csv = os.path.join(file_folder,'r_' + subreddit + '-' + str(scrape_time) + '.csv')
    with open(filepath_use, 'r') as fp:
        reader = csv.reader(fp)
        for row in reader:
            ids.append(row[0])
    idlist = list(set(ids))
    subreddit = r.subreddit('Opiates')
    for id in idlist:
        bad_id = get_all_comments(id, file_folder)
        bad_id_list.append(bad_id)

def scrape_all_comments(subreddit,file_folder):
    build_id_list(subreddit,file_folder)
    with open(filepath_csv, 'w') as f:
        writer = csv.writer(f)
        submission.comments.replace_more(limit=0)
        comment_queue = submission.comments[:]
        for comment in submission.comments.list():
            id = comment.id
            url = submission.url
            num_comments = submission.num_comments
            parent_id = comment.parent_id
            text = comment.body
            author = comment.author
            utc = comment.created_utc
            a=(id,url,num_comments,parent_id,text,author,utc)
            writer.writerow(a)
    return idlist
    return bad_id_list

def get_all_comments(submission_id_string,file_folder):
try:
    print(submission_id_string)
    print(type(submission_id_string))
    if isinstance(submission_id_string, str) is False:
        print("in loop")
        submission_id_string = submission_id_string.decode('utf-8')
        print(submission_id_string)
        assert isinstance(submission_id_string, str)
    subreddit = r.subreddit('Opiates')
    submission = r.submission(id=submission_id_string)
    scrape_time = calendar.timegm(time.gmtime())
    subreddit ='Opiates_' + str(submission)
    filepath_csv = os.path.join(file_folder,'r_' + subreddit + '-' + str(scrape_time) + '.csv')
    check_for_more = 1
    while check_for_more is 1:
        num_mores = 0
        for comment in submission.comments:
            if isinstance(comment, MoreComments):
                num_mores = num_mores + 1
        if num_mores is 0:
            check_for_more = 0
        else :
            submission.comments.get_comments(limit=None, threshold=0)
    with open(filepath_csv, 'w') as f:
        writer = csv.writer(f)
        submission.comments.replace_more(limit=0)
        comment_queue = submission.comments[:]
        for comment in submission.comments.list():
            id = comment.id
            url = submission.url
            num_comments = submission.num_comments
            parent_id = comment.parent_id
            text = comment.body
            author = comment.author
            utc = comment.created_utc
            a=(id,url,num_comments,parent_id,text,author,utc)
            writer.writerow(a)
except:
    return submission_id_string