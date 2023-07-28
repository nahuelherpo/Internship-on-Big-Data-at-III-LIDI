# Internship-on-Big-Data-at-III-LIDI

I have got a Spark based big data processing system up and running, got it up and running on an available cluster, done performance testing and bug detection.

## Table of Contents

1. [Introduction](#introduction)
2. [TreeSpark tool](#treespark-tool)
3. [Work done](#work-done)
    - [I. Enviroment](#i-enviroment)
    - [II. Data generation](#ii-data-generation)
    - [III. Performance analysis](#iii-performance-analysis)
    - [IV. Data validation](#iv-data-validation)
4. [Additional Resources](#additional-resources)


## Introduction

The professional internship was carried out at the III-LIDI (LIDI Instituto de Investigaciones Informáticas), Research, Development and Transfer Unit that works in the Faculty of Informatics of the National University of La Plata. Several projects are carried out within the Institute, this work is part of the Intelligent Systems project.

The tutor assigned to the internship collaborated with members of the Animal Reproduction Research Institute (INIRA) of the Faculty of Veterinary Sciences of the National University of La Plata, in a project on the fetal programming of cows. The result of this collaboration was the TreeSpark tool. This internship consisted of continuing with the development of said tool.

In this internship we worked on the TreeSpark framework, which is still in the development and testing phase. The objective was to continue the work on TreeSpark, mainly through functionality testing and performance measurement. Based on these objectives, the following tasks were carried out: execution environment configuration, error detection, data generation for tests, performance analysis, validation of the data, among other tasks.

## TreeSpark tool

TreeSpark is an open source tool to carry out progeny analysis and provides functionality that allows simple access to the information of the individuals and their relations both as progenitors and descendants. This tool is developed as a Python module, which in turn inherits the distributed processing features of Spark, allowing it to process large volumes of progeny information.


## Work done

### I. Enviroment

To test the operation and performance of TreeSpark, tests were executed in a cluster. Said cluster is made up of 4 nodes, a master and three slaves; each node features 8GB of RAM and a 4th generation Intel Core i3 processor. The data was stored in a distributed filesystem HDFS (Hadoop Distributed File System) which is available in a docker container that runs on top of the cluster.
Because the framework and the tests were developed with Python and other libraries, the Poetry tool was used for version control and installation of the necessary packages. To see the versions used see the "pyproject.toml" file.

### II. Data generation

The `generate_data.sh` script, developed in Bash, generates a random data set. This is made up of two tables: the individuals table that stores information on the cow and the production table that stores for each zero cow, one or more lactation production records.

The script can optionally receive the following parameters (the order must be respected):
 - TREE_DEPTH: Indicates the maximum depth that each tree can have.
 - NUM_OF_TREES: Indicates the number of families of cows to generate.
 - MAX_FILES_SIZE: Indicates the maximum size of the database in KB.
 
### III. Performance analysis

For performance analysis, the Python program `main.cluster.py` was built, it executes a query in TreeSpark and measures the execution time of the query.

The `spark_run.sh` Bash script runs the `main.cluster.py` script with the query 30 times, on different databases. The different databases were uploaded to HDFS.

### IV. Data validation

To validate the results, the `validator.py` script was built in Python. This script compares the results of the solution on Pandas and TreeSpark.

## Additional Resources

 * Paper of TreeSpark: [article site](http://sedici.unlp.edu.ar/handle/10915/130340)
 * Poetry tutorial: [site](https://realpython.com/dependency-management-python-poetry/)

