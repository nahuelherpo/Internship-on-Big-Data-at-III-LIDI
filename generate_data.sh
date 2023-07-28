#!/bin/bash
#
# Random Database Generator
#
# Author: Herpo Nahuel
#
# Date: Dec, 2022
#
#
# This script generates  a random database for later
# queries. The database contains two tables: cow and
# production.  The  cows  have a family relationship
# (in the form of a tree),  where each cow that is a
# mother has associated records  of milk production.
# For more details, see the README.MD file.
#
#

##########################################################
#TO RUN THIS BASH SCRIPT USE THE NEXT COMMAND
#./generate_data.sh
#./generate_data.sh 5 (5 leves of tree depth)
#./generate_data.sh 2 5 (2 depth, 5 trees)
#./generate_data.sh 2 5 100 (2 depth, 5 trees and 100KB of max size)
##########################################################

#GLOBAL VARIABLES
ID_COUNT_COWS=0     #Counter to assign id, 0 belongs to the oldest cow
ID_COUNT_PROD=0     #ids for individuos and produccion, respectively
TREE_DEPTH=1        #Tree depth in levels
NUM_OF_TREES=6      #Number of trees to create, each tree will go in a file

#Function to create productions
function createCowProductionFor {
    local ID_COW=$1
    #The tree number I work on
    local TREE_NUM=$2
    #The number of production registers of the cow
    local NUM_OF_PROD_REG=$((1 + $RANDOM % 100)) # 1-100 register per cow
    for i in `seq ${NUM_OF_PROD_REG}`
    do
        #Generate the random number of production (milk production litros)
        local MILK_PROD_L=$((1 + $RANDOM % 55)) # 1-55 litros of milk
        #print the row on produccion.csv
        #ID_PROD,ID_VAC,FECHA,NRO_PROD
        echo "${ID_COUNT_PROD},${ID_COW},01/01/1900,${MILK_PROD_L}" >> datos/producciones/produccion${TREE_NUM}.csv
        let "ID_COUNT_PROD++"
    done
}

#Function to create children
function createChildrenFor {
    #Check that it has not exceeded the space limit
    SIZE_OF_FILES=`du -k -s datos/ | awk '{print $1}'`
    if [ $SIZE_OF_FILES -ge $MAX_FILES_SIZE ]
    then
	#If it already reaches the size, abort the program
	exit
    fi
    #I keep the id of the mother of the children that are possibly created
    local ID_MOTHER=$1
    #I store the depth in a local variable
    local DEPTH=$2
    #The tree number I work on
    local TREE_NUM=$3
    let "DEPTH--"
    if [ "${DEPTH}" -eq 0 ]
    then
        echo "Limit of depth" >> log.txt
        echo "@--------------------------------------" >> log.txt
    else
        #If you haven't reached the depth limit yet
        #I generate a number random between 0 and 4, to indicate the number of children to create
        local CHILDREN_NUM=$((0 + $RANDOM % 5)) # 0 1 2 3 o 4 hijas
        echo "The cow $ID_MOTHER has $CHILDREN_NUM children" >> log.txt
        #If it has no children I don't keep calling recursively
        if [ "${CHILDREN_NUM}" -eq 0 ]
        then
            echo "The cow ${ID_MOTHER} has not children" >> log.txt
            echo "@--------------------------------------" >> log.txt
        else
            #If she has children then at some point she produced milk
            #Therefore I generate the milk production records for that cow
            createCowProductionFor $ID_MOTHER $TREE_NUM
            #I generate the children for the mother
            echo "Generating children for cow $ID_MOTHER" >> log.txt
            #For from i=0 to i<CHILDREN_NUM, to create the children
            for i in `seq ${CHILDREN_NUM}`
            do
                local COW_ORDER=$i
                local COW_ID=$ID_COUNT_COWS
                let "ID_COUNT_COWS++"
                echo "The cow ${COW_ID} is daughter of ${ID_MOTHER}" >> log.txt
                echo "${COW_ID},01/01/1900,1,${ID_MOTHER},${COW_ORDER}" >> datos/individuos/individuos${TREE_NUM}.csv
                #I create the daughters for this daughter (recursion)
                createChildrenFor $COW_ID $DEPTH $TREE_NUM
            done
        fi
    fi
}

#Create the individuos and produccion files,
#the number of files is predefined
function createFiles {
    #I remove if it already exists
    rm -r datos
    #I create the datos folder
    mkdir datos
    mkdir datos/individuos
    mkdir datos/producciones
    #Create the individuos and produccion files (csv files)
    for i in `seq ${NUM_OF_TREES}`
    do
        touch datos/individuos/individuos${i}.csv
        touch datos/producciones/produccion${i}.csv
        echo "ID,FECHA_NAC,VIVE_EN,ID_MADRE,ORDEN" > datos/individuos/individuos${i}.csv
        echo "ID_PROD,ID_VAC,FECHA,NRO_PROD" > datos/producciones/produccion${i}.csv
    done
}

#Create the individuos and produccion files,
#the number of files is predefined
function createOldestCow {
    #Create N cows depending of trees number
    for i in `seq ${NUM_OF_TREES}`
    do
        echo "---------------------------------------" >> log.txt
        echo "Creating the oldest cow for tree ${i}" >> log.txt
        #ID,FECHA_NAC,VIVE_EN,ID_MADRE,ORDEN
        local ID_MOTHER_OF_ALL=$ID_COUNT_COWS
        let "ID_COUNT_COWS++"
        echo "${ID_MOTHER_OF_ALL},01/01/1899,1,,1" >> datos/individuos/individuos${i}.csv
        #Generate the milk production for the first mother of the tree
        createChildrenFor $ID_MOTHER_OF_ALL $TREE_DEPTH $i
        echo "---------------------------------------"
    done
}

#Check the script parameters
#The order of the parameters must be respected
case "$#" in
    0)  #If it is zero the values are set by default
        TREE_DEPTH=6
        NUM_OF_TREES=1
	    MAX_FILES_SIZE=2097152 #2GB
        echo "The tree depth is 6 (default value)"
        echo "The number of trees is 1 (default value)"
	    echo "The max files size is 2GB (default value)"
        ;;
    1)
        TREE_DEPTH=$1
        NUM_OF_TREES=1
	    MAX_FILES_SIZE=2097152 #2GB
        echo "The tree depth is ${TREE_DEPTH}"
        echo "The number of trees is 1 (default value)"
	    echo "The max files size is 2GB (default value)"
        ;;
    2)
        TREE_DEPTH=$1
        NUM_OF_TREES=$2
	    MAX_FILES_SIZE=2097152 #2GB
        echo "The tree depth is ${TREE_DEPTH}"
        echo "The number of trees is ${NUM_OF_TREES}"
	    echo "The max files size is 2GB (default value)"
        ;;
    3)
	    TREE_DEPTH=$1
        NUM_OF_TREES=$2
	    MAX_FILES_SIZE=$3
        echo "The tree depth is ${TREE_DEPTH}"
        echo "The number of trees is ${NUM_OF_TREES}"
	    echo "The max files size is ${MAX_FILES_SIZE}KB"
	    ;;
    *)
        echo "Bad arguments"
        exit 0
        ;;
esac

echo "---------------------------------------"

createFiles
createOldestCow
