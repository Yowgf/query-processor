#!/bin/bash

set -e

index_file=$1
queries_file=$2
ranker=$3
other_args=$4

python3 processor.py -i $index_file -q $queries_file -r $ranker $other_args
