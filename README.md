recsys
======

Use those scripts to download the 10M movielens dataset and index them in Elasticsearch:

setup-env.sh
============

Takes care of setting up an environment that can be used to index movielens data. Fetches:

* Movielens 10M dataset
* hetrec dataset to augment movielens
* Elasticsearch 1.4* for indexing
* Marvel (+ installs Marvel into Elasticsearch) for query exploration
* kibana for data exploration


post_movies.py
===============

    usage: post_movies.py [-h] [--lens lens] [--clear clearance] [--stop clearonly]
    
    Parse movielens formatted information and post message therein to a running elasticsearch instance.
    
    optional arguments:
    -h, --help         show this help message and exit
    --lens lens        Path to movielens directory in local filesystem.
    --clear clearance  Set to "true" to clear the existing index before re-indexing.
    --stop clearonly   Only clear index, do not add more documents.
  
Index names used:

* movies - movie information
* ratings - for each rating information on the user, rating value and title of the rated movie
* tags - tags with timestamp and user information
  
post_movie_details.py
=====================
  
    usage: post_movie_details.py [-h] [--datadir datadir] [--clear clearance] [--stop clearonly]
    
    Parse hetrec formatted information and post details therein to a running elasticsearch instance. Index used:  movie_details
    
    optional arguments:
    -h, --help         show this help message and exit
    --datadir datadir  Path to data directory in local filesystem.
    --clear clearance  Set to "true" to clear the existing index before re-indexing.
    --stop clearonly   Only clear index, do not add more documents.
