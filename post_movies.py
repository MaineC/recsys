#!/usr/bin/env python
# vim: set fileencoding=utf-8 :
#
# MovieLens indexing tool for generating Elasticsearch indices.
#
# Re-write of an original idea/script by Isabel Drost-Fromm.
# Copyright 2014 © Thilo Fromm
#
# This file is licensed to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from pprint import pprint
import elasticsearch

import argparse
import string
import sys
import os
import json

from threading import Thread
from Queue import Queue


def parse(fname, field_types, custom_append=None):
    """Parse a data file from the Movie Lens data set.

       This function parses a file from the Movie Lens data set.
       The function returns a generator for JSON dict strings.

       Optionally, a callback for extending the JSON dict string can be passed.
       The callback function will be called for each line parsed. Fields parsed
       will be provided via an array passed to the callback.
       The callback signature thusly is:

        custom_append(fields)

       Arguments:
       fname -- file name of the data file.
       field_types -- array of field identifiers. Must match the number of
                       fields per line.
                       NOTE: The "Genres" field entries will be put in an
                       array in the return dict.

       Example Usage:
        for line in parse('ratings.dat',
                            ("UserID", "MovieID", "Rating", "Timestamp"):
            print line

       Example output:
        {"UserID":"1","MovieID":"122","Rating":"5","Timestamp":"838985046"}
        {"UserID":"1","MovieID":"362","Rating":"5","Timestamp":"838984885"}
        {"UserID":"1","MovieID":"364","Rating":"5","Timestamp":"838983707"}
        ...

       Exaample for "movies.dat":
        for line in parse('movies.dat', ("MovieID", "Title", "Genres")):
            print line

       Output:
        {"MovieID":"65130","Title":"Revolutionary Road (2008)","Genres":["Drama","Romance"]}
        ...
    """
    if not custom_append:
        custom_append = lambda x: ""
    sz = os.stat(fname).st_size
    rd = 0
    with open(fname) as f:
        num_fields = len(field_types)
        for line in f: 
            line = line.strip()
            fields = line.split("::")
            ret = "{"
            for i in range(num_fields):
                val = fields[i]
                if field_types[i] == "Genres":
                    g   = fields[i].split("|")
                    ret = ret + '"Genres":["%s"],' % '","'.join(g)
                elif field_types[i] == "Timestamp":
                    ret = ret + '"%s":%s000,' % (field_types[i], val)
                else:
                    ret = ret + '"%s":%s,' % (field_types[i], json.dumps(val))
            rd = rd + len(line)
            ret += custom_append(fields)
            yield ret[:-1] + '}', rd, sz


def index_writer(es, q, index, doctype):
    """Reads data tuple from queue
        (start-document-num, end-document-num, documents-buf,
            bytes-read, bytes-total),
       writes documents to elasticsearch, and prints progress. Function is
       intended to run in a separate thread.

       Arguments:
       es      -- elasticsearch client instance to index data into
       q       -- Queue instance to read from
       index   -- Elasticsearch index
       doctype -- Elasticsearch doctype
    """
    while True:
        data = q.get()
        if data == "quit":
            break
        c_start, counter, buf, read, total = data
        try:
            es.bulk(buf)
            if read:
                sys.stdout.write("\r   %s %% done (%s of %s KiB, %s documents)"
                     % (read*100/total, read / 1024, total / 1024, counter))
                sys.stdout.flush()
        except Exception, e:
            print "Indexing error: skipping lines %s-%s." % (c_start, counter)
            print e
        q.task_done()


def index_file(es, fname, field_types, index, doctype,
                parse_append_cb=None, qlen=50, lines_per_bulk=10000):
    """Parse a movielens data file and write the result JSON dicts to
        elastisearch in a separate thread.

       Arguments:
       es        -- pyes 'es' instance to index data into
       fname       -- data file name to parse
       field_types -- data file field types, see parse() documentation
       index       -- Elasticsearch index
       doctype     -- Elasticsearch doctype

       Keyword arguments:
       parse_append_cb -- optional parser callback, see parse() documentation
       qlen            -- Max number of bulk writes to queue
       lines_per_bulk  -- Number of lines per bulk write
    """
    q = Queue(maxsize=qlen)
    es.bulk_size = 1
    t = Thread(target=index_writer, args=(es, q, index, doctype))
    t.start()

    counter = 0
    c_start = 0
    buf     = ""
    header = '{"index": {"_index": "%s", "_type": "%s"}}' %(index, doctype)

    print "Indexing %s" % index
    for line, read, total in parse(fname,field_types, parse_append_cb):
        counter = counter + 1
        buf = buf + "%s\n%s\n" % (header, line)
        if counter % lines_per_bulk == 0:
            q.put((c_start, counter, buf, read, total))
            c_start = counter
            buf = ""
    if c_start < counter:
        q.put((c_start, counter, buf, total, total))
    q.put("quit")
    t.join()
    print ""


def delete_indices(es):
    """Delete indices.

       Arguments
       es -- ES client instance
    """
    print "Deleting indices."
    es.indices.delete(index='movies', ignore=404)
    es.indices.delete(index='tags', ignore=404)
    es.indices.delete(index='ratings', ignore=404)
    es.indices.delete(index='users', ignore=404)

def create_mappings(es):
    """Re-create indices, create mappings.

       Arguments
       es -- ES client instance
    """
    print "Creating indices and mappings."
    es.indices.create(index='movies', ignore=400)
    es.indices.create(index='tags', ignore=400)
    es.indices.create(index='ratings', ignore=400)
    es.indices.create(index='users', ignore=400)

    ts_mapping = {     'Timestamp' : { 'boost': 1.0, 'type': 'date'} }
    ratings_mapping = {'Timestamp' : { 'boost': 1.0, 'type': 'date'}, 
                       'Rating'    : { 'boost': 1.0, 'type': 'float'},
                       'Title'     : { "type": "string",
                                          "fields": { "raw" : {
                                                      "type": "string",
                                                      "index": "not_analyzed"
                                                } } } }
    users_mapping = {  'UserID' :{'type':'string'},
                       'Ratings':{
                            "type":"nested",
                            "properties":{
                                'Rating'   :{'boost':1.0,'type':'float'},
                                'Timestamp':{'boost':1.0,'type':'date'},
                                'Title'    :{"type":"string","fields":{
                                                  "raw" : {
                                                    "type": "string",
                                                    "index": "not_analyzed"}}}}}}
    def put(es, doc, mappings):
        es.indices.put_mapping(doc, {doc: {'properties':mappings}}, doc+'s')
    put(es, "movie", ts_mapping)
    put(es, "rating", ratings_mapping)
    put(es, "tag", ts_mapping)
    put(es, "user", users_mapping)


def cmdl_args():
    """Parse command line arguments

       Returns
        argparse instance ready to use
    """
    parser = argparse.ArgumentParser(
                description='Parse movielens formatted information'
                 + ' and post message therein to a running elasticsearch'
                 + ' instance.')
    parser.add_argument('--lens', metavar='lens', dest='lens', default='.',
        help='Path to movielens directory in local filesystem.')
    parser.add_argument('--clear', metavar='clearance', dest='clear',
        help='Set to "true" to clear the existing index before re-indexing.')
    parser.add_argument('--stop', metavar='clearonly', dest='clearonly',
        help='Only clear index, do not add more documents.')

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    """ This script will parse MovieLens' 'movies.dat', 'ratings.dat', and
        'tags.dat', and generate corresponding indices in Elasticsearch.
        Additionally, a user-centric 'users' index is created (one doc per user
        w/ all the movies in it rated by that user).
        Also, the 'ratings' index is extended with the movie name.

        Note that the 'users' index uses nested documents to store the movies
        rated per user. You will need to specify the nested path in your
        queries, e.g.:

        GET users/_search?search_type=count
        {
          "query": {
            "nested": {
              "path": "Ratings",
              "query": {
                "bool": {"must" : [
                        {"range": {"Ratings.Rating": {"gte": 4}}},
                        {"match_phrase": {"Ratings.Title": "Planet Terror"}} ]
            }}}},
          "aggs": {
            "Ratings" : {
                "nested": { "path": "Ratings"},
                "aggs"  : { "Title" : {
                    "significant_terms":{"field" : "Ratings.Title.raw"}}}}}}

        to run a significant terms aggregation on movies rated by users who 
        rated "Planet Terror" a "4" ("good") or better.
        """
    args = cmdl_args()

    es = elasticsearch.Elasticsearch()

    if args.clear == 'true':
        delete_indices(es)
        if args.clearonly == 'true':
            sys.exit()

    create_mappings(es)

    # Generate "users" index w/ movies rated per user
    #  This index is generated on the fly when parsing 'ratings.dat'
    user_id=None
    ratings_buf= ""
    users_buf= ""
    users_scount = 0
    users_count = 0
    users_header = '{"index": {"_index": "users", "_type": "user"}}'
    users_bulk = 500
    users_q = Queue(50)
    users_es = elasticsearch.Elasticsearch()
    users_es.bulk_size = 1
    users_t = Thread(target=index_writer,
                     args=(users_es, users_q, "users", "user"))
    users_t.start()

    # Extract movie titles when parsing 'movies.dat'
    titles = {}
    def extract_titles(fields):
        titles[fields[0]] = json.dumps(fields[1])
        return ""

    # Callbakc to generate 'users' index and append movie titles
    #  to ratings documents. 'users' index was inspired by a script
    #  by Mark Karwood.
    def gen_users_and_append_titles(fields):
        global user_id, ratings_buf, users_buf, users_scount, users_count
        if user_id and user_id != fields[0]:
            users_buf = users_buf + '%s\n%s\n' \
                        % (users_header, '{"UserID":"%s","Ratings":[%s]}' \
                                             % (fields[0], ratings_buf[:-1]))
            ratings_buf = ''
            users_count = users_count + 1
            if users_count % users_bulk == 0:
                users_q.put((users_scount,users_count,users_buf,None,None))
                users_buf = ''
        user_id = fields[0]
        rating = '{"MovieID": "%s", "Title":%s, "Rating":"%s"},'         \
                             % (fields[1], titles[fields[1]], fields[2])
        ratings_buf = ratings_buf + rating
        return '"Title":%s ' % titles[fields[1]]

    # Parse movies, ratings, and tags
    index_file(es, os.path.join(args.lens, 'movies.dat'),
                ("MovieID", "Title", "Genres"), 'movies', 'movie',
                extract_titles)
    sys.stdout.write("Generating + Indexing 'users', ")
    # this will also geerate the 'users' index
    index_file(es, os.path.join(args.lens,'ratings.dat'),
            ("UserID", "MovieID", "Rating", "Timestamp"), 'ratings', 'rating',
                gen_users_and_append_titles)
    index_file(es, os.path.join(args.lens,'tags.dat'),
            ("UserID", "MovieID", "Tag", "Timestamp"), 'tags', 'tag')

    # Write the last user document
    users_buf = users_buf + '%s\n%s\n' \
                % (users_header, '{"UserID":"%s","Ratings":[%s]}\n' \
                                             % (user_id, ratings_buf[:-1]))
    users_q.put((users_scount,users_count,users_buf,None,None))
    users_q.put("quit")
    users_t.join()

