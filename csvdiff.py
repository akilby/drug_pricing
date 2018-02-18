import Quandl
import pandas as pd

df = Quandl.get('FMAC/HPI_AK')
print(df.head())
fifty_states = pd.read_html('https://en.wikipedia.org/wiki/List_of_states_and_territories_of_the_United_States')
print(fifty_states[0][1])
for abbv in fifty_states[0][1]:
    print("FMAC/HPI_%s" % str(abbv))



import csv
input_file1 = "Desktop/test2/opiates_7x1k1c-1518663169 copy.csv"
input_file2 = "Desktop/test2/opiates_7x1k1c-1518914343 copy.csv"
output_file = "Desktop/test2/opiates_7x1k1c-final.csv"
a = open(input_file1, "r")
output_file = open(output_file, "w")
output_file.close()
count = 0

with open(input_file1, "r") as f1:
    with open(input_file2, "r") as f2:
        reader1 = csv.reader(f1)
        reader2 = csv.reader(f2)
        diff_rows = (row1 for row1, row2 in zip(reader1, reader2) if row1 != row2)
        with open(output_file, 'w') as fout:
            writer = csv.writer(fout)
            writer.writerows(diff_rows)

set1 = list(set(csv1))
set2 = list(set(csv2))
print set1 - set2 # in 1, not in 2
print set2 - set1 # in 2, not in 1
print set1 & set2 # in both
with open('test.csv', 'w') as f:
    fieldnames = ['']

def separate_unique_and_dup_files(master_subfolder, working_subfolder, sep='-'):
    """Separates comment files that are complete duplicates, and puts them in a dups folder which can later be purged"""
    # full_file_list = glob.glob(os.path.join(folder, '*')) + glob.glob(os.path.join(archive_subfolder, '*')) + glob.glob(os.path.join(working_subfolder, '*'))
    # full_file_list1 = glob.glob(os.path.join(archive_subfolder, '*')) + glob.glob(os.path.join(working_subfolder, '*'))
    full_file_list = glob.glob(os.path.join(master_subfolder, '*'))
    prefix_list = list(set([x.split(sep)[0].split('/')[-1] for x in full_file_list]))
    move_to_dups = 0
    move_to_master_archive = 0
    total_count = len(prefix_list)
    j = 0
    for prefix in prefix_list:
        j += 1
        #if not os.path.isdir(os.path.join(working_subfolder, prefix)):
        new_master_list, discard_list = uniquify_prefix(prefix, master_subfolder, working_subfolder)
        print('uniquified prefix %s out of %s' % (j, total_count))
        for filename in new_master_list:
                #if os.path.normpath(os.path.dirname(filename)) == os.path.normpath(folder):
            shutil.move(filename, master_subfolder)
            move_to_master_archive += 1
            print('%s moved to master folder' % filename)
        #for filename in discard_list:
            #if os.path.normpath(os.path.dirname(filename)) == os.path.normpath(folder):
        #    move_to_dups += 1
    print("moving complete")
    # os.rmdir(working_subfolder)  
    remaining_files = len(glob.glob(os.path.join(working_subfolder, '*')))
    print('Moved %s comment files to permanent archive; %s duplicate comment files remain' % (move_to_master_archive, remaining_files))



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
                with open("Users/jackiereimer/Dropbox/r_opiates/comments/all_comments.csv", "w") as f:
                    writer = csv.writer(f)
                    for row in row_list:
                        if 
    return new_master_list, discard_list





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