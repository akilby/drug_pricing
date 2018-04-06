test_locations = ['turin', 'milano']
test_comments = ['This is a comment about milan.', 'This is a comment about turin.', 'This is a comment about manufacturing.']


'''This works given the sample above and is robust to the 'turin' problem'''
location = test_locations[0]
word = re.compile(r'\b%s\b' % location, re.I)
comment = test_comments[0]
y = word.search(comment)
y


'''The function below iterates through both lists and produces a final list of present locations and their location
'''

def find_location_comments(test_comments,test_locations):
    location_comments = []
    for comment, location in [(comment,location) for comment in test_comments for location in test_locations]:
        word = re.compile(r'\b%s\b' % location, re.I)
        y = word.search(comment)
        print(y)
        if y != None:
        	location_comments.append(y)
    print(location_comments)