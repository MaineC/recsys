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

# put kibana on top
wget https://download.elasticsearch.org/kibana/kibana/kibana-3.1.1.zip
unzip kibana-3.1.1.zip
rm kibana-3.1.1.zip
mv kibana-3.1.1 ~/public_html/kibana


# start elasticsearch
./demo/es/elasticsearch/elasticsearch-1.3.4/bin/elasticsearch -f
