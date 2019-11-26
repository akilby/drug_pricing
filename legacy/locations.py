import csv
import itertools


filename = '/Users/akilby/Dropbox/Drug Pricing Project/locations/AdWords.csv'


def make_locations_list_from_adwords(filename):
    with open(filename, 'r') as in_file:
        location_file = csv.reader(in_file)
        locations = list(location_file)
    locations = [x[:5] for x in locations]
    locations = [x[:2] + x[2].split(',') + x[3:] for x in locations]
    locations = list(itertools.chain.from_iterable(locations))
    locations = list(set(locations))
    return locations

