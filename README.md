# MinoanER
Minoan ER is an Entity Resolution (ER) framework, built by researchers in Crete (the land of the ancient Minoan civilization). Entity resolution aims to identify descriptions that refer to the same entity within or across knowledge bases. 

The website of the project is http://www.csd.uoc.gr/~vefthym/minoanER/ 

The functionality of this framework is described in details in the followig PhD thesis (mostly in Chapter 4): <br/>
http://csd.uoc.gr/~vefthym/DissertationEfthymiou.pdf

MinoanER is implemented in Java 8+, using Apache Spark. We assume that a Spark cluster is available. Our code has been tested in a Spark cluster with HDFS and Mesos.

The steps followed by MinoanER are Blocking, Meta-blocking and Matching. 
Currently, the step of (token) blocking is taken from https://github.com/vefthym/ERframework/blob/master/src/NewApproaches/ExportDatasets.java
but it can be easily incorporated in this repository, as a Spak task, as well. 


# Running MinoanER
The main file is https://github.com/vefthym/MinoanER/blob/master/src/main/java/minoaner/workflow/Main.java. 
As documented in this file, it assumes 5 input paths and 1 output path, taken as runtime arguments: 

<b>inputBlocking:</b> <br/> 
The resulting blocks from token blocking. You can generate such a file from https://github.com/vefthym/ERframework/blob/master/src/NewApproaches/ExportDatasets.java.
Each line corresponds to a block and its contents. The formatting should be: <br/>
blockId <i>TAB</i> entityIdFromD1<i>#</i>entityIdFromD1<i>#</i> ... <i>;</i>entityIdFromD2<i>#</i>entityIdFromD2<i>#</i> ... <br/>
All those Ids should be positive integers. 

<b>inputTriples1/2:</b> <br/>
The raw RDF triples of the first/second KB in N-triples format (without the trailing " ." part).

<b>entityIds1/2:</b> <br/>
To save some space, we replace all entity URLs with numeric (positive integer) ids. This file contains this mapping that you should provide. Each line corresponds to one mapping and should be in the form: <br/>
entityURL <i>TAB</i> numericId <br/>
The same numericId should not be assigned to two different entityURLs and the entityURLs should be the ones appearing in the raw RDF input (inputTriples1/2). 

<b>outputPath:</b> <br/>
The (HDFS) path in which the output mappings will be stored. The format of the generated output is: <br/>
entityIdFromD1 <i>TAB</i> entityIdFromD2 <br/>
for each pair of entities that have been found to match. <br/>
WARNING: the outputPath directory is deleted on each run. 

You can find examples of datasets used in MinoanER in our project's website: http://csd.uoc.gr/~vefthym/minoanER/datasets.html.


# Setup and Tuning

You can tune the Spark session parameters (number of workers, executors, memory, parallelism, etc) by calling the setUpSpark method in https://github.com/vefthym/MinoanER/blob/master/src/main/java/minoaner/utils/Utils.java. The body of this method should be adjusted to reflect the resources of your Spark cluster. 

In the main method, you will find some hardcoded attributes that act as entity names (labels) for the datasets that we have tested. 
Those attributes have been generated automatically by getting the top attributes of each KB based on the harmonic mean of support and discriminability (see related publications). 
You can hardcode the corresponding attributes for your KBs, or find them automatically by calling the methods found in the class https://github.com/vefthym/MinoanER/blob/master/src/main/java/minoaner/relationsWeighting/RelationsRank.java.


