#!/bin/bash

#Generate files of 100KB, 1MB, 100MB and 1GB...
./generate_data.sh 8 2 100
mv datos/ datos1/
./generate_data.sh 12 2 1024
mv datos/ datos2/
./generate_data.sh 16 4 102400
mv datos/ datos3/
./generate_data.sh 22 16 1048576
mv datos/ datos4/
