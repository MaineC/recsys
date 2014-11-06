#!/usr/bin/env python

from pprint import pprint
from pyes import *

import argparse
import string
import sys
import os


def parse_ratings(fname="ratings.dat"):
    """Parse a 'ratings' data file from the Movie Lens data set.

       This function parses a ratings file from the Movie Lens data set.
       Optionally, a file name can be provided. The function returns a
       generator for JSON dict strings.

       Keyword arguments:
       fname -- file name of the ratings file. Defaults to 'ratings.dat'.

       Example Usage:
        for line in parse_ratings():
            print line

       Example output:
        {"UserID":"1","MovieID":"122","Rating":"5","Timestamp":"838985046"}
        {"UserID":"1","MovieID":"362","Rating":"5","Timestamp":"838984885"}
        {"UserID":"1","MovieID":"364","Rating":"5","Timestamp":"838983707"}
        ...
    """
    with open(fname) as f:
        for line in f: 
            line = line.strip()
            fields = line.split("::")
            yield {"UserID":fields[0],"MovieID":fields[1],"Rating":fields[2],"Timestamp":fields[3]}


def parse_tags(fname="tags.dat"):
    """Parse a 'tags' data file from the Movie Lens data set.
       
       Please refer to parse_ratings() documantation for detailed reference.

       Example output:
        {"UserID":"20","MovieID":"3033","Tag":"star wars","Timestamp":"1188263880"}
        ...
    """
    with open(fname) as f:
        for line in f: 
            line = line.strip()
            fields = line.split("::")
            yield {"UserID":fields[0],"MovieID":fields[1],"Tag":fields[2],"Timestamp":fields[3]}


def parse_movies(fname="movies.dat"):
    """Parse a 'movies' data file from the Movie Lens data set.
       
       Please refer to parse_ratings() documantation for detailed reference.
       Note that the "Genres" dict value is an array.

       Example output:
        {"MovieID":"1","Title":"Toy Story (1995)","Genres":["Adventure","Animation","Children","Comedy"]}
        ...
    """
    with open(fname) as f:
        for line in f: 
            line = line.strip()
            fields = line.split("::")
            g = fields[2].split("|")
            g_string = '["%s"]' % '","'.join(g)
            yield {"MovieID":fields[0],"Title":fields[1],"Genres":fields[2]}





if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Parse movielens formatted information and post message therein to a running elasticsearch instance.')
    parser.add_argument('--lens', metavar='lens', dest='lens',
        help='Path to movielens directory in local filesystem.')
    parser.add_argument('--clear', metavar='clearance', dest='clear',
        help='Set equal to true to clear the existing and running index before re-indexing.')
    parser.add_argument('--stop', metavar='clearonly', dest='clearonly',
        help='Only clear index, do not add more documents.')

    args = parser.parse_args()

    conn = ES('http://127.0.0.1:9200')
    if(args.clear == 'true'):
        try:
            conn.indices.delete_index('movies')
        except:
            pass

        conn.indices.create_index('movies')
    if(args.clearonly == 'true'):
        sys.exit()

    mapping = {
        'title': {
            'boost': 1.0,
            'index': 'analyzed',
            'store': 'yes',
            'type': 'string',
            'term_vector': 'no'},
        'genre': {
            'boost': 1.0,
            'index': 'not_analyzed',
            'store': 'yes',
            'type': 'string',
            'term_vector': 'no'},
    }
    conn.indices.put_mapping("movie", {'properties': mapping}, ['movies'])

    counter = 0

    for movie in parse_movies(os.path.join(args.lens,'movies.dat')):
        print "Indexing document with id: " + movie['MovieID']
        try:
            conn.index(movie, 'movies', 'movie')
        except:
            print "Skipping doc with id: " + str(counter)
            pass
        counter = counter + 1

