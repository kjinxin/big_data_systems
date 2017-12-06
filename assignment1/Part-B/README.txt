This is the readme file for Part-B: the AnagramSorter application.

The java source file is named AnagramSorter.java. In total, there are four classes defined as follows:

Map: generate key/value pairs for the input.txt. The key/value pair is (alphabetical order of word A, word A).

Reduce: generate lists of words, whose alphabet are identical.

There is a intermediate output file for the above.

Map2: take as input the intermediate otput file, and generates the key/value pair. THe pair appears as (number of words in list A, list A).

Reduce2: no reduce is done and the only functionality is to sort the list by key in desecinding order.

The way to compile the jave source file is as follows:
 
Set up path:
source /run.sh
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH$JAVA_HOME/lib/tools.jar

Compile:
hadoop com.sun.tools.javac.Main AnagramSorter.java
jar cf ac.jar AnagramSorter*.class

Before running the program, upload the input.txt to remote HDFS file system by the following command:
hadoop fs -put ~/Part-B/input.txt /input

Run:
hadoop jar ac.jar AnagramSorter /input /output

