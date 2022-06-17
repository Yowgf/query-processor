#!/bin/bash

set -e

word=$1

[ -z "$word" ] && exit 1

search=$(cat index.out | grep "^${word} " | sed 's/^[^ ]* //' | awk -v FS=, -v RS=' ' '{print $1}')
grep -r "${search}" urlmapping
