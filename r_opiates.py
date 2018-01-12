
# virtual environment "beautifulsoup"

import csv
import time
import calendar
import os
import datetime
import praw
userJ = "scrape submissions to " + "/r/opiates using praw/python/terminal. By /user/jrreimer"

r = praw.Reddit(client_id = '2do-OPn-K3ii3A',
				client_secret = 'pBqEsDdGCk-E_n55vkb0ITFzl1Y',
				password = '~KilbyAssignment',
				username = 'jrreimer',
				user_agent = userJ)


###################################################################################################################################

# FUNCTIONAL PRAW CODE


# pulls comments within a specific post

# input parameters
os.chdir('/Users/jackiereimer/Documents/Opioids/dumps/comments')
file_folder = "/Users/jackiereimer/Documents/Opioids/dumps/comments/"
from praw.models import MoreComments

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
		subreddit_scrape ='Opiates_' + str(submission)	
		filepath_csv = os.path.join(file_folder,'r_' + subreddit_scrape + '-' + str(scrape_time) + '.csv')
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

# pulls all top level threads between given time frame

# input parameters

subreddit_scrape = "opiates"
file_folder = "/Users/jackiereimer/Documents/Opioids/dumps/threads/"
iterate_over_days = 1
iterate_over = 86400*iterate_over_days
start_date_string = 'January 1, 2018 UTC'
end_date_string = 'January 12, 2018 UTC'

def scrape_subreddit(iterate_over,start_date_string,end_date_string,subreddit_scrape,file_folder):
	start_date = calendar.timegm(time.strptime(start_date_string, '%B %d, %Y UTC'))
	if end_date_string is 'today':
		end_date = calendar.timegm(time.gmtime()) + iterate_over
	else:
		end_date = calendar.timegm(time.strptime(end_date_string, '%B %d, %Y UTC')) + iterate_over
	filepath_csv = file_folder + "r_" + subreddit_scrape + "_" + str(start_date) + "_" + str(end_date) + ".csv"
	print(filepath_csv)
	i = 0
	with open(filepath_csv, 'w') as f:
		writer = csv.writer(f)
		for iteration in range(start_date,end_date,iterate_over):
			i1 = iteration
			i2 = iteration + iterate_over
			timestampstring = "timestamp:" + str(i1) + ".." + str(i2)
			j=0
			for submission in r.subreddit('Opiates').search(timestampstring, sort="new", syntax="cloudsearch", limit=None):
				id = submission.id
				url = submission.url
				num_comments = submission.num_comments
				short_link = submission.shortlink
				author = submission.author
				selftext = submission.selftext
				title = submission.title
				utc = submission.created_utc
				a=(id,url,num_comments,short_link,author,title,selftext,utc)
				writer.writerow(a)
				i = i+1
				j = j+1
				if j>500:
					f.close()
					os.remove(filepath_csv)
					raise Exception("Submissions greater than 500.")
			time.sleep(2)
			print(j)
		print(i)

###################################################################################################################################

# FUNCTIONAL DATA MANAGEMENT CODE

# Collects all scraped posts into single csv

def all_submissions_database():
	row_list = []
	for dumpname in os.listdir("/Users/jackiereimer/Documents/Opioids/dumps/threads/"):
		filepath_use = "/Users/jackiereimer/Documents/Opioids/dumps/threads/" + dumpname
		if not dumpname.startswith('.'):
			with open(filepath_use, 'r') as f:
				reader = csv.reader(f)
				for row in reader:
					a = tuple(row)
					row_list.append(a)
	row_list = list(set(row_list))
	#print row_list 
	with open("/Users/jackiereimer/Documents/Opioids/dumps/threads/all_dumps.csv", 'w') as f:
		writer = csv.writer(f)
		for row in row_list:
			a = list(row) 
			writer.writerow(a)

#Collects all scraped comments into single csv


def all_comments_database():
	row_list = []
	for commentname in os.listdir("/Users/jackiereimer/Documents/Opioids/dumps/comments/"):
#		thread_details = [commentname.partition("_")[0], commentname.partition("_")[2].partition(".")[0]]
		filepath_use = "/Users/jackiereimer/Documents/Opioids/dumps/comments/" + commentname
		if not commentname.startswith('.'):
			with open(filepath_use, 'r') as f:
				reader = csv.reader(f)
				for row in reader:
#					b = thread_details + row
#					a = tuple(b)
					a = tuple(row)
					row_list.append(a)
	row_list = list(set(row_list))	
	#print row_list 
	with open("/Users/jackiereimer/Documents/Opioids/dumps/comments/all_comments.csv", 'w') as f:
		writer = csv.writer(f)
		for row in row_list:
			a = list(row) 
			writer.writerow(a)

#Returns all scraped data related to comments and threads containing key term (case sensitive)

def submission_has_word(word):
	i = 0
	for line in open("/Users/jackiereimer/Documents/Opioids/dumps/threads/all_dumps.csv"):
		if word in line:
			print(line)
			i = i + 1
	return i

def comment_has_word(word):
	i = 0
	for line in open("/Users/jackiereimer/Documents/Opioids/dumps/comments/all_comments.csv"):
		if word in line:
			print(line)
			i = i + 1
	return i


def comment_has_words(word1,word2):
	i = 0
	for line in open("/Users/jackiereimer/Documents/Opioids/dumps/comments/all_comments.csv"):
		if word1 in line:
			if word2 in line:
				print(line)
				i = i + 1
	return i

#Returns all scraped data of a certain type

def get_usernames():
	usernames = []
	for comment_thread in os.listdir("/Users/jackiereimer/Documents/Opioids/dumps/comments/"):
		filepath_use = "/Users/jackiereimer/Documents/Opioids/dumps/comments/" + comment_thread
		if not comment_thread.startswith("."):
			with open(filepath_use, 'r') as fp:
				reader = csv.reader(fp)
				for row in reader:
					usernames.append(row[3])
	for submission in os.listdir("/Users/jackiereimer/Documents/Opioids/dumps/threads/"):
		filepath_use = "/Users/jackiereimer/Documents/Opioids/dumps/threads/" + submission
		if not submission.startswith("."):
			with open(filepath_use, 'r') as fp:
				reader = csv.reader(fp)
				for row in reader:
					usernames.append(row[4])
	usernames = list(set(usernames))
	return usernames

def get_submission_ids(subreddit):
	ids = []
	for dumpname in os.listdir("/Users/jackiereimer/Documents/Opioids/dumps/threads/"):
		filepath_use = "/Users/jackiereimer/Documents/Opioids/dumps/threads/" + dumpname
		if dumpname.startswith('r_'+ subreddit_scrape):
			with open(filepath_use, 'r') as fp:
				reader = csv.reader(fp)
				for row in reader:
					ids.append(row[0])
	idlist = list(set(ids))
	return(idlist)


###################################################################################################################################

# WORK IN PROGRESS CODE


def comment_has_regexp(regexp):
	i = 0
	for line in open("/Users/jackiereimer/Documents/Opioids/dumps/comments/all_comments.csv"):
		if re.search(regexp,line):
			print(line)
			i = i + 1
	return(i)


def new_and_old_submission_ids():
	all_ids = get_submission_ids(subreddit)
	ever_scraped_thread_ids = []
	for comment_thread in os.listdir("/Users/jackiereimer/Documents/Opioids/dumps/comments/"):
		ever_scraped_thread_ids.append(comment_thread.partition("_")[0])
	ever_scraped_thread_ids = list(set(ever_scraped_thread_ids))
	unscraped_thread_ids = [x for x in all_ids if x not in ever_scraped_thread_ids]
	return(ever_scraped_thread_ids,unscraped_thread_ids)
#	print(ever_scraped_thread_ids,unscraped_thread_ids)

def update_comment_threads_to_fresh(time_difference):	
	# time_difference is the threshold for the maximum time between when the thread was scraped, and the newest comment scraped, to see if it should be scraped again
	candidate_threads = new_and_old_submission_ids[0]

def scrape_all_comments(subreddit,file_folder):
	ids = []
	bad_id_list = []
	filepath_use = "/Users/jackiereimer/Documents/Opioids/dumps/threads/r_opiates_1512950400_1513641600.csv"
	filepath_csv = os.path.join(file_folder,'r_' + subreddit_scrape + '-' + str(scrape_time) + '.csv')
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

##############################################################################

def get_all_comments(submission_id_string,file_folder):
	print(submission_id_string)
	subreddit = r.subreddit('Opiates')
	submission = r.submission(id=submission_id_string)
	scrape_time = calendar.timegm(time.gmtime())
	subreddit_scrape ='Opiates_' + str(submission)	
	filepath_csv = 'r_' + subreddit_scrape + '-' + str(scrape_time) + '.csv'
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
	scrape_time = calendar.timegm(time.gmtime())
	filepath_use = "/Users/jackiereimer/Documents/Opioids/dumps/threads/r_opiates_1514764800_1515801600.csv"
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
###################################################################################################################################

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

###################################################################################################################################
# Problems to work out:
	# In update_comment_threads_to_fresh
	# 	Ever_scrapes_thread_ids:  what does this return? 
	# 	unscraped_thread_ids: 	what does this return?
	# In Comment_has_words:
	#	Is this looking for both words in same comment?
	# Currently, get_submission_ids and unscraped_thread_ids return the same thing :(
	# How to set time_difference
	# What am I getting when I try to run scrape_all_comments




###################################################################################################################################

# Input Parameters

# # for comment_id in get_submission_ids():
# #	get_all_comments(comment_id,"/Users/akilby/Dropbox/Research/Data/heroin_prices/comments_by_thread/")

for year in range(2009,2009):
	start_date_string = "January 1, " + str(year) + " UTC"
	end_date_string = "January 1, " + str(year+1) + " UTC"
	print(start_date_string,end_date_string)

###################################################################################################################################

# Notes

# Goal 1 Scrape everything from day 1
# Earliest historic scrape 	January 1, 2008
# Latest   historic scrape 	December 19, 2017

# Goal 2 Scrape everyhing being posted now


