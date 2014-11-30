#!/bin/bash

#setup ES
wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.3.4.zip
mkdir -p demo/es
mv elasticsearch-1.3.4.zip demo/es
unzip demo/es/elasticsearch-1.3.4.zip -d demo/es/elasticsearch

# install marvel for sense into elasticsearch
./demo/es/elasticsearch/elasticsearch-1.3.4/bin/plugin -i elasticsearch/marvel/latest

# deal with movielens data
wget http://files.grouplens.org/datasets/movielens/ml-10m.zip
mkdir -p demo/data
mv ml-10m.zip demo/data
unzip demo/data/ml-10m.zip -d demo/data/ml-10m

wget http://files.grouplens.org/datasets/hetrec2011/hetrec2011-movielens-2k-v2.zip
mkdir -p demo/augmented-data
mv hetrec2011-movielens-2k-v2.zip demo/augmented-data
unzip demo/augmented-data/hetrec2011-movielens-2k-v2.zip

# download kibana
wget https://download.elasticsearch.org/kibana/kibana/kibana-4.0.0-BETA1.1.zip
mkdir -p demo/kibana
mv kibana-4.0.0-BETA1.1.zip demo/kibana
unzip demo/kibana/kibana-4.0.0-BETA1.1.zip

# start elasticsearch
./demo/es/elasticsearch/elasticsearch-1.3.4/bin/elasticsearch -f
