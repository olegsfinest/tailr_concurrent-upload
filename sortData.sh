#!/bin/bash

for dir in ./*/
do
    dir=${dir%*/}
    # $folder =  ${dir##*/}
    # gzip -dc $folder/data.nq.gz | rev | LC_ALL=C sort -u -S 20G --parallel=16 | rev | gzip > $folder/data_sorted.nq.gz
    gzip -dc ${dir##*/}/data.nq.gz | rev | LC_ALL=C sort -u -S 20G --parallel=16 | rev | gzip > ${dir##*/}/data_sorted.nq.gz
    echo ${dir##*/} sorted
done