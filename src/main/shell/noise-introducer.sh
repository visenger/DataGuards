#!/bin/sh
exec scala -savecompiled "$0" "$@"
!#

for d in 1 10 20 40 50 80 100
    do
    for n in 2 4 6 8 10
	   do
	   echo "here introduce noise $n into $d data set"
	   done
    done

echo $?