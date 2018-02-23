import csv
from filecmp import dircmp
import time
import glob
import os


def complete_comment_files(master_subfolder, complete_comment_folder, sep='-'):
    '''this is the most complete function'''
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
            writer = csv.writer(outfile)
            writer.writerows(master_row_list)
            
            

# def complete_comment_files(master_subfolder, complete_comment_folder, sep = '-'):
# '''this is the most complete function'''    
#    full_file_list = glob.glob(os.path.join(master_subfolder, '*'))
#    prefix_list = list(set([x.split(sep)[0].split('/')[-1] for x in full_file_list]))
#    for file in full_file_list:
#        for prefix in prefix_list: 
#        filepath_use = os.path.join(master_subfolder, '*')
#        filepath_complete = os.path.join(complete_comment_folder, prefix)
#        with open(filepath_use,'r') as in_file, open(filepath_complete.csv,'w') as out_file:
#            writer = csv.writer(out_file)
#            seen = set()
#            for row in in_file:
#                if row in seen: 
#                    pass # will this skip over previously added comments or will this 
#                else:
#                    seen.add(row)
#                    writer.writerow(row)
#        print(seen)



def combine_complete_and_master(prefix, master_subfolder, complete_subfolder, sep='-'):
'''this compares current complete files to master file'''
    full_master_file_list = glob.glob(os.path.join(master_subfolder, '*'))
    full_complete_file_list = glob.glob(os.path.join(complete_subfolder, '*'))
    master_prefix_list = list(set([x.split(sep)[0].split('/')[-1] for item in full_master_file_list]))
    complete_prefix_list = list(set([x.split(sep)[0].split('/')[-1] for item in full_complete_file_list]))
    for master_prefix in master_prefix_list:
        for complete_prefix in complete_prefix_list:
            if master_prefix == complete_prefix:
                filepath_use = os.path.join(master_comment_folder, master_prefix)
                filepath_current_complete = os.path.join(complete_comment_folder, '*')
                filepath_complete = os.path.join(complete_comment_files, complete_prefix + str(time.time()))
                for item in filepath_use
                with open(filepath_use, 'r') as in_file_1, open(filepath_current_complete, 'r') as in_file_2, open(filepath_complete, 'w') as out_file:
                    writer = csv.writer(out_file)
                    seen = set()
                    for row in in_file_2
                        if row in seen: 
                            pass
                        else:
                            seen.add(row)
                    for row in in_file_1:
                        if row in seen: 
                            pass # will this skip over previously added comments?
                        else:
                            seen.add(row)
                    writer.writerow(seen) # Is this d
            else:
                with open(filepath_use, 'r') as in_file, open(filepath_current_complete, 'w') as out_file:
                    writer = csv.writer(out_file)
                    seen = set()
                    for row in out_file:
                        if row in seen:
                            pass
                        else:
                            seen.add(row)
                    for row in in_file:
                        if row in seen:
                            pass
                        else:
                            seen.add(row)
                    writer.writerow(seen)





def uniquify_prefix(prefix, master_subfolder, complete_subfolder, sep='-'):
    full_master_list = glob.glob(os.path.join(master_subfolder, '*'))
    full_complete_list = glob.glob(os.path.join(complete_subfolder, '*'))
    master_prefix_list = list(set([x.split(sep)[0].split('/')[-1] for x in full_master_list]))
    complete_prefix_list = list(set([x.split(sep)[0].split('/')[-1] for x in full_complete_list]))
    discard_list = []
    new_complete_list = []
    for item in prefix_file_list_master:
        duplicate = False
        for complete_item in prefix_file_list_complete:
            if duplicate is False:
                if filecmp.cmp(complete_item, item):
                    duplicate = True
        if duplicate:
            discard_list.append(item)
        else:
            new_complete_list.append(item)
            combine_complete_and_master()
            with open(filepath_use,'r') as in_file, open(filepath_complete.csv,'w') as out_file:
                writer = csv.writer
                seen = set() # set for fast O(1) amortized lookup
    for row in in_file:
        if row in seen: pass # will this skip over previously added comments or will this 
    seen.add(row)
    out_file.write(row)
    print(seen)
                writer = csv.writer(f)
                for row in row_list:
                    if 
    return new_master_list, discard_list


#Deprecated code 
#
#
#def move_composing_files():
#    full_file_list = glob.glob(os.path.join(master_subfolder, '*'))
#    
#    full_conent_file_list = []
#    for prefix in full_file_list:
#        generate_complete_file()
#
#
#def generate_complete_file():
#    prefix_file_list1 = glob.glob(os.path.join(master_subfolder, '%s*' % prefix))
#    prefix_file_list2 = glob.glob(os.path.join(master_subfolder, '%s*' % prefix))
#    if prefix not in full_conent_file_list:
#        for item in prefix_list:
#            if duplicate is False:
#                if filecmp.cmp() 
#            for master_item in prefix_file_list_master:
#                if duplicate is False:
#                    if filecmp.cmp(master_item, item):
#                        duplicate = True
#
#    discard_list = []
#    new_master_list = []
#    if prefix_file_list_master != []:
#        for item in prefix_file_list_working:
#            duplicate = False
#            for master_item in prefix_file_list_master:
#                if duplicate is False:
#                    if filecmp.cmp(master_item, item):
#                        duplicate = True
#            if duplicate:
#                discard_list.append(item)
#            else:
#                new_master_list.append(item)
#                with open("Users/jackiereimer/Dropbox/r_opiates/comments/all_comments.csv", "w") as f:
#                    writer = csv.writer(f)
#                    for row in row_list:
#                        if 
#    return new_master_list, discard_list
#
#
#input_file1 = "Desktop/test2/opiates_7x1k1c-1518663169 copy.csv"
#input_file2 = "Desktop/test2/opiates_7x1k1c-1518914343 copy.csv"
#output_file = "Desktop/test2/opiates_7x1k1c-final.csv"
#a = open(input_file1, "r")
#output_file = open(output_file, "w")
#output_file.close()
#count = 0
#
#with open(input_file1, "r") as f1:
#    with open(input_file2, "r") as f2:
#        reader1 = csv.reader(f1)
#        reader2 = csv.reader(f2)
#        diff_rows = (row1 for row1, row2 in zip(reader1, reader2) if row1 != row2)
#        with open(output_file, 'w') as fout:
#            writer = csv.writer(fout)
#            writer.writerows(diff_rows)
#
#set1 = list(set(csv1))
#set2 = list(set(csv2))
#print set1 - set2 # in 1, not in 2
#print set2 - set1 # in 2, not in 1
#print set1 & set2 # in both
#with open('test.csv', 'w') as f:
#    fieldnames = ['']
#
#def separate_unique_and_dup_files(master_subfolder, working_subfolder, sep='-'):
#    """Separates comment files that are complete duplicates, and puts them in a dups folder which can later be purged"""
#    # full_file_list = glob.glob(os.path.join(folder, '*')) + glob.glob(os.path.join(archive_subfolder, '*')) + glob.glob(os.path.join(working_subfolder, '*'))
#    # full_file_list1 = glob.glob(os.path.join(archive_subfolder, '*')) + glob.glob(os.path.join(working_subfolder, '*'))
#    full_file_list = glob.glob(os.path.join(master_subfolder, '*'))
#    prefix_list = list(set([x.split(sep)[0].split('/')[-1] for x in full_file_list]))
#    move_to_dups = 0
#    move_to_master_archive = 0
#    total_count = len(prefix_list)
#    j = 0
#    for prefix in prefix_list:
#        j += 1
#        #if not os.path.isdir(os.path.join(working_subfolder, prefix)):
#        new_master_list, discard_list = uniquify_prefix(prefix, master_subfolder, working_subfolder)
#        print('uniquified prefix %s out of %s' % (j, total_count))
#        for filename in new_master_list:
#                #if os.path.normpath(os.path.dirname(filename)) == os.path.normpath(folder):
#            shutil.move(filename, master_subfolder)
#            move_to_master_archive += 1
#            print('%s moved to master folder' % filename)
#        #for filename in discard_list:
#            #if os.path.normpath(os.path.dirname(filename)) == os.path.normpath(folder):
#        #    move_to_dups += 1
#    print("moving complete")
#    # os.rmdir(working_subfolder)  
#    remaining_files = len(glob.glob(os.path.join(working_subfolder, '*')))
#    print('Moved %s comment files to permanent archive; %s duplicate comment files remain' % (move_to_master_archive, remaining_files))
#
#
#
#def uniquify_prefix(prefix, master_subfolder, working_subfolder):
#    prefix_file_list_working = glob.glob(os.path.join(working_subfolder, '%s*' % prefix))
#    prefix_file_list_master = glob.glob(os.path.join(master_subfolder, '%s*' % prefix))
#    discard_list = []
#    new_master_list = []
#    if prefix_file_list_master != []:
#        for item in prefix_file_list_working:
#            duplicate = False
#            for master_item in prefix_file_list_master:
#                if duplicate is False:
#                    if filecmp.cmp(master_item, item):
#                        duplicate = True
#            if duplicate:
#                discard_list.append(item)
#            else:
#                new_master_list.append(item)
#                with open("Users/jackiereimer/Dropbox/r_opiates/comments/all_comments.csv", "w") as f:
#                    writer = csv.writer(f)
#                    for row in row_list:
#                        if 
#    return new_master_list, discard_list
#
#
#
#
#
#def all_submissions():
#    row_list = []
#    for dumpname in os.listdir("/Users/jackiereimer/Dropbox/r_opiates Data/threads/"):
#        filepath_use = "/Users/jackiereimer/Dropbox/r_opiates Data/threads/" + dumpname
#        if not dumpname.startswith('.'):
#            with open(filepath_use, 'r') as f:
#                reader = csv.reader(f)
#                for row in reader:
#                    a = tuple(row)
#                    row_list.append(a)
#    row_list = list(set(row_list))
#    #print row_list
#    with open("/Users/jackiereimer/Dropbox/r_opiates Data/threads/all_dumps.csv", 'w') as f:
#        writer = csv.writer(f)
#        for row in row_list:
#            a = list(row)
#            writer.writerow(a)
#
#def all_comments():
#    row_list = []
#    for commentname in os.listdir("/Users/jackiereimer/Dropbox/r_opiates Data/comments/"):
##       thread_details = [commentname.partition("_")[0], commentname.partition("_")[2].partition(".")[0]]
#        filepath_use = "/Users/jackiereimer/Dropbox/r_opiates Data/comments/" + commentname
#        if not commentname.startswith('.'):
#            with open(filepath_use, 'r') as f:
#                reader = csv.reader(f)
#                for row in reader:
##                   b = thread_details + row
##                   a = tuple(b)
#                    a = tuple(row)
#                    row_list.append(a)
#    row_list = list(set(row_list))
#    #print row_list
#    with open("/Users/jackiereimer/Dropbox/r_opiates Data/comments/all_comments.csv", 'w') as f:
#        writer = csv.writer(f)
#        for row in row_list:
#            a = list(row)
#            writer.writerow(a)
