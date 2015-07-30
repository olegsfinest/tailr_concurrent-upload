#!/bin/bash

#TODO file as input param
gzip -dc /Users/magnus/Datasets/dyldo/2012-05-06/data.nq.gz | rev | sort | rev | gzip > /Users/magnus/Datasets/dyldo/2012-05-06/data_sorted.nq.gz