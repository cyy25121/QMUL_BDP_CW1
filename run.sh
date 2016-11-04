#!/bin/sh

if [[ $# != 1 ]]; then
	exit
fi

if [ $1 == "build" ]; then
	cd pb ; make clean; make
elif [ $1 == "run" ]; then
	cd pb ; hadoop-moonshot jar dist/TwitterTimeAnalysis.jar TwitterTimeAnalysis /data/olympictweets2016rio out
elif [ $1 == "clean" ]; then
	rm -rf out/
fi
