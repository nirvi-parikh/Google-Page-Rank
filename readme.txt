// Name: Nirvi Parikh
// Student ID: 801022018
// Email: nparikh4@uncc.edu
// Assignment 3: PageRank Implementation

Running on DSBA Cluster===========================================

1)$ sudo su hdfs
2)$ hadoop fs -chown nparikh4 /user/nparikh4
3)$ exit
4) Creating a directory for Assignment 3 as Assg3
	$ hadoop fs -mkdir /user/nparikh4/Assg3

5) Creating a directory for input files
    	$ hadoop fs -mkdir /user/nparikh4/Assg3/input

6) Adding simplewiki-20150901-pages-articles-processed.xml in this input directory
	$ hadoop fs -put simplewiki-20150901-pages-articles-processed.xml /user/nparikh4/Assg3/input

7) Compile the PageRank.java file
	$ mkdir -p build
	$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* PageRank.java -d build -Xlint 

8) Creating a jar file
	$ jar -cvf PageRank.jar -C build/ .

9) Running jar on input files
	$ hadoop jar PageRank.jar pagerank.PageRank /user/nparikh4/Assg3/input /user/nparikh4/Assg3/output

10)Reading the output
	$ hadoop fs -cat /user/nparikh4/Assg3/output/*

11)Copying output to local
	$ hadoop fs -copyToLocal /user/nparikh4/Assg3/output/* .

***** Delete all intermediate and final output files before running any program again, if there are any.
hadoop fs -rm -r /user/cloudera/Assg3/output*
 
