#!/bin/sh

if [[ $# != 1 ]]; then
	exit
fi

if [ $1 == "build" ]; then
	cd pa ; make clean; make
elif [ $1 == "run" ]; then
	cd pa ; hadoop-moonshot jar dist/TwitterContentAnalysis.jar TwitterContentAnalysis /data/olympictweets2016rio out
elif [ $1 == "clean" ]; then
	rm -rf out/
fi
