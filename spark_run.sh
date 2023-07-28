#!/bin/bash

# Off hdfs safemode...
hdfs dfsadmin -safemode leave

for i in `seq 4`
do
	for r in `seq 30`
	do
		spark-submit --py-files Clases.py main.py $i
	done
done