#!/bin/bash
#
# Spark launcher
#
# Author: Herpo Nahuel
#
# Date: Dec, 2022
#
#
# This script runs the Spark application 30 times
# per DB. The reason for the repetition is to take
# the average time, for later performance analysis.
#
#


# Off hdfs safemode...
hdfs dfsadmin -safemode leave

for i in `seq 4`
do
	for r in `seq 30`
	do
		spark-submit --py-files Classes.py main.cluster.py $i
	done
done