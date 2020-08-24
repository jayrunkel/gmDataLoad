#!/bin/bash

previous=0
while read n; do
    diff=$((n - previous))
    if [ $diff -gt 0 ]
    then 
	ts=`date +"%T"`
	echo "$ts: Transaction: $diff  : $n - $previous"
	previous=$n
    fi
done
