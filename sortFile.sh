#!/bin/bash

#TODO file as input param
gzip -dc /Volumes/Microschrott/linkedData/data.nq.gz | rev | sort | rev | gzip > /Volumes/Microschrott/linkedData/data_sorted.nq.gz