#!/usr/bin/env python
# vim: set fileencoding=utf-8 :
#
# Yet another movieLens/hetrec indexing tool for generating Elasticsearch
#  indices.
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

from pyes import *

import argparse
import sys
import traceback
import os
import json

from threading import Thread
from Queue import Queue


def index_writer(con, q, bulk):
    """Reads data tuple from queue
        (documents-buf, line-num, bytes-read, bytes-total),
       writes documents to elasticsearch, and prints progress. Function is
       intended to run in a separate thread.

       Arguments:
       conn    -- pyes 'es' instance to index data into
       q       -- Queue instance to read from
    """
    while True:
        data = q.get()
        if data == "quit":
            conn.bulker.flush_bulk(forced=True)
            break
        buf, lines, read, total = data
        try:
            conn.index_raw_bulk("", buf)
            if read:
                sys.stdout.write("\r   %s %% done (%s of %s KiB, %s documents)"
                     % (read*100/total, read / 1024, total / 1024, lines))
                sys.stdout.flush()
        except Exception, e:
            print "Indexing error: skipping lines %s-%s." % (lines - bulk,
                                                                        lines)
            print e
        q.task_done()


class index_file(file):
    """Helper class which returns lines starting with a specific index number
        from a properties file"""
    def __init__(self, *args, **kwargs):
        super(index_file, self).__init__(*args,**kwargs)
        self.__remainder_line = []
        self.__lnum = 0

    def lines_with_idx(self, idx):
        """Return all lines that start with the number 'idx'. 
           Stop reading from file when a line w/ number != 'idx' is
           encountered, and buffer this line for succeeding access.

           Arguments:
           idx -- index (number) the lines are supposed to start with

           Returns:
           array of arrays: one entry per line, which is a nested array, which
                            has one entry per field in the line (split by '\\t')
              e.g.
              [
                ["1", "erik_von_detten", "Erik von Detten", "13"],
                ["1", "greg-berg", "Greg Berg", "17"],
                ...
              ]
       """
        ret = []
        if self.__remainder_line:
            if int(self.__remainder_line[0]) == idx:
                ret.append(self.__remainder_line)
            else:
                return ""

        for l in self:
            self.__lnum += 1
            try:
                l = l.strip().split('\t')
                i = int(l[0])
            except Exception, e:
                if self.__lnum > 1:
                    print "%s: Error parsing line %s. Skipping." %(
                                                        self.name, self.__lnum) 
                    print traceback.format_exc()
                continue

            if idx == i:
                ret.append(l)
            else:
                self.__remainder_line = l
                break
        return ret


def index(conn, datadir, tag_names, qlen=50, lines_per_bulk=1000):
    """Parse hetrec data set and write the result JSON dicts to
        elastisearch in a separate thread.

       Arguments:
       conn        -- pyes 'es' instance to index data into
       fname       -- data directory

       Keyword arguments:
       qlen            -- Max number of bulk writes to queue
       lines_per_bulk  -- Number of lines per bulk write
    """
    act = index_file(os.path.join(datadir, "movie_actors.dat"))
    cnt = index_file(os.path.join(datadir, "movie_countries.dat"))
    drc = index_file(os.path.join(datadir, "movie_directors.dat"))
    gen = index_file(os.path.join(datadir, "movie_genres.dat"))
    loc = index_file(os.path.join(datadir, "movie_locations.dat"))
    tag = index_file(os.path.join(datadir, "movie_tags.dat"))

    buf     = ""
    header = '{"index": {"_index": "movie_details", "_type": "movie_detail"'
    q = Queue(maxsize=qlen)
    conn.bulk_size = 1
    trd = Thread(target=index_writer, args=(conn, q, lines_per_bulk))
    trd.start()

    movie_fn  = os.path.join(datadir, "movies.dat")
    bytes_tot = os.stat(movie_fn).st_size
    bytes_rd  = 0
    lines_read= 0

    with open(movie_fn) as movie_f:
        for line in  movie_f:
            lines_read += 1
            try:
                bytes_rd += len(line)
                line  = line.strip().split('\t')
                idx   = int(line[0])

                try:    cnty = json.dumps(cnt.lines_with_idx(idx)[0][1],
                                                            encoding="latin1")
                except: cnty = ""
                try:    drcr = json.dumps(drc.lines_with_idx(idx)[0][2],
                                                            encoding='latin1')
                except: drcr = ""
                acts = json.dumps([ a[2] for a in act.lines_with_idx(idx) ],
                                                            encoding='latin1')
                gens = json.dumps([ a[1] for a in gen.lines_with_idx(idx) ],
                                                            encoding='latin1')
                locs = json.dumps( [ " ".join(a[1:5])
                                            for a in loc.lines_with_idx(idx) ]
                                    , encoding="latin1") 
                tags = json.dumps([ tag_names[int(t[1])]
                                            for t in tag.lines_with_idx(idx)],
                                                            encoding='latin1')
                mdata = ( '{ "title":%s, "year":"%s","country":%s,"director":%s,'
                         +'  "actors":%s,"genres":%s,"locations":%s,"tags":%s}' ) \
                        % ( json.dumps(line[1], encoding='latin1'), line[5],
                                cnty, drcr, acts, gens, locs, tags )
            except Exception, e:
                if lines_read > 1:
                    print "Parse / assemble error in line %s: %s" % (
                                                            lines_read, line)
                    print traceback.format_exc()
                continue
            buf = buf + '%s,_id:"%s"}}\n%s\n' % (header, idx, mdata)

            if lines_read % lines_per_bulk == 0:
                q.put((buf, lines_read, bytes_rd, bytes_tot))
                buf = ""
    if lines_read % lines_per_bulk != 0:
        q.put((buf, lines_read, bytes_rd, bytes_tot))
    q.put("quit")
    trd.join()
    print ""


def parse_tags(datadir, fname):
    """Parse 'tags' file and return a dict w/ tag indices for keys, tag names
        for values.
    """
    ret     = {}
    skipped = []
    lnum    = 1

    with open(os.path.join(datadir, fname)) as t:
        for line in t:
            try:
                line = line.strip().split('\t')
                ret[ int(line[0]) ] = str(line[1])
            except:
                skipped.append(lnum)
            lnum += 1
    return ret, skipped


def cmdl_args():
    """Parse command line arguments

       Returns
        argparse instance ready to use
    """
    parser = argparse.ArgumentParser(
                description='Parse movielens formatted information'
                 + ' and post message therein to a running elasticsearch'
                 + ' instance.')
    parser.add_argument('--datadir', metavar='datadir', dest='datadir',
        help='Path to data directory in local filesystem.')
    parser.add_argument('--clear', metavar='clearance', dest='clear',
        help='Set to "true" to clear the existing index before re-indexing.')
    parser.add_argument('--stop', metavar='clearonly', dest='clearonly',
        help='Only clear index, do not add more documents.')

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    """ This script will parse the hetrec2011 dataset, creating a
        "movie_details" index in ES on the fly.

        Here's an example on using mlt to query the movie_details index.
        The first query will return the _id(s) of a movie based on its title - 
        and yes, the hetrec data set seems to include the same movies multiple
        times, featuring multiple IDs.

        GET movie_details/_search
        {
          "query": {
              "match_phrase": {"title": "Planet Terror"}}}

        GET movie_details/_search
        {
          "query": {
            "more_like_this" : {
                "fields" : ["genres"],
                "ids" : [54995, 8903 ],
                "min_term_freq" : 1,
                "max_query_terms" : 5
            }}
        }
    """
    args = cmdl_args()
    conn = ES('http://127.0.0.1:9200', bulker_class=models.ListBulker)

    if args.clear == 'true':
        try:
            conn.indices.delete_index('movie_details')
        except:
            pass
        conn.indices.create_index('movie_details')
        if args.clearonly == 'true':
            sys.exit()

    sys.stdout.write("Parsing tags..."); sys.stdout.flush()
    tags, skipped = parse_tags(args.datadir, "tags.dat")
    sys.stdout.write("Done, skipped %s lines.\n" % (len(skipped) - 1))

    sys.stdout.write("Parsing + Indexing movie details")
    index(conn, args.datadir, tags)

