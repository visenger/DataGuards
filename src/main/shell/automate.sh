#!/bin/bash

	   for i in 4 6 8 10
	   do
	   		cat $i/part* > $i/tpch-$i.db
	   		scp -r  $i/tpch-$i.db  larysa@worker2-1.mia.tu-berlin.de:~/rockit/TPC-H/$i
	   done

echo $?