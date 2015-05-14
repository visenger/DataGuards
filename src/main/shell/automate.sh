#!/bin/bash

	   for i in 2 4 6 8 10 12 14 16 18 20
	   do
	   		#cat $i/part* > $i/tpch-$i.db
	   		scp -r  $i/*  larysa@worker2-1.mia.tu-berlin.de:~/rockit/EXPERIMENTS-2/HOSP-2
	   done

echo $?