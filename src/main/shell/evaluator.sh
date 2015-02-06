#!/bin/sh

cd ..
cd scala/de/util

echo "running Playground...."
scalac Playground.scala
scala Playground

#
for d in 1 10 20 40 50 80 100
    do
    for n in 2 4 6 8 10
	   do
	   for g in 0.1 0.01 0.001 0.0001
            do
            echo "-new evaluation round-"
	   		#echo " evaluation for data size $d"
	   		#echo "- evaluation for noisy percent $n"
	   		#echo "-- evaluation for inference gap parameter $g"
	   		done
       done
    done

echo $?
