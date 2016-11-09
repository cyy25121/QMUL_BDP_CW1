#!/bin/sh

if [[ $# != 2 ]]; then
	if [ $1 == "clean" ]; then
		cd pa; make clean;
		cd ../pb; make clean;
		cd ../pc; make clean;
	else
		exit
	fi
fi

if [ $1 == "pa" ]; then
	if [ $2 == "build" ]; then
		cd pa ; make clean; make
	elif [ $2 == "run" ]; then
		cd pa ; hadoop-moonshot jar dist/TwitterContentAnalysis.jar TwitterContentAnalysis /data/olympictweets2016rio out out2
	elif [ $2 == "clean" ]; then
		rm -rf out/
	fi
elif [ $1 == "pb" ]; then
	if [ $2 == "build" ]; then
		cd pb ; make clean; make
	elif [ $2 == "run" ]; then
		cd pb ; hadoop-moonshot jar dist/TwitterTimeAnalysis.jar TwitterTimeAnalysis /data/olympictweets2016rio out
	elif [ $2 == "clean" ]; then
		rm -rf out/
	fi
elif [ $1 == "pc" ]; then
	if [ $2 == "build" ]; then
		cd pc ; make clean; make
	elif [ $2 == "run" ]; then
		cd pc ; hadoop-moonshot jar dist/TweetsHashtagAnalysis.jar TweetsHashtagAnalysis /data/olympictweets2016rio out1 out2 out3 out4
	elif [ $2 == "runl" ]; then
		cd pc ; hadoop-moonshot jar dist/TweetsHashtagAnalysis.jar TweetsHashtagAnalysis input/olympictweets2016rio.test out1 out2 out3 out4
	elif [ $2 == "clean" ]; then
		rm -rf out/
	fi

fi
