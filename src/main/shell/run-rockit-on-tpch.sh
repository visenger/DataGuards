#!/bin/bash

	   for i in 2 4 6 8 10
	   do
	   		for j in 500 1000 10000 20000 30000 40000 50000 70000 90000 100000
	   		do
				sudo service mysql restart
				echo "star logging for noise $i% and TPC-H data size $j"

				LOGFILE=$i/$j/results/results-tpch-dataSize-$j-noise-$i.txt 
			   	echo "star logging for noise $i% and TPC-H data size $j" > $LOGFILE

			   	START=$(($(date +%s%N)/1000000))
					# start your script work here
					
				    #java -jar /opt/rockit/rockit-0.5.277.jar -input tpch-interleaved.mln -data $i/$j/tpch-dataSize-$j-noise-$i.db -output $i/$j/results/output-tpch-dataSize-$j-noise-$i.db >> $LOGFILE
				    java -jar /opt/rockit/rockit-0.5.277.jar -input tpch-2.mln -data $i/$j/tpch-dataSize-$j-noise-$i.db -output $i/$j/results/output-tpch-dataSize-$j-noise-$i.db >> $LOGFILE

					# your logic ends here
				END=$(($(date +%s%N)/1000000))
				DIFF=$(( $END - $START ))
				echo "It took $DIFF milliseconds" >> $LOGFILE
			done   
	   done

echo $?