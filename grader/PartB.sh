#!/bin/bash
source /home/ubuntu/run.sh
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH$JAVA_HOME/lib/tools.jar
cd /home/ubuntu/Part-B
hadoop com.sun.tools.javac.Main AnagramSorter.java
jar cf ac.jar AnagramSorter*.class
hdfs dfs -rmr /output
hdfs dfs -rmr /tempoutput
hadoop jar ac.jar AnagramSorter /input /output
