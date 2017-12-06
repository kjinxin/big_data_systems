This is the program folder for Part A. 
We have three application files written in python, PartAQuestion1.py, PartAQuestion2.py and PartAQuestion3.py. 
There are also the streaming-emulator shell script (streaming-emulator.sh) and the target user list (input-list.csv) for question 3.

To run an application, go to the current directory, and then type:
~/software/spark-2.0.0-bin-hadoop2.6/bin/spark-submit ./PartAQuestion1.py parameter1, parameter2, ...

The directory where data are stored is ./split-dataset, which is already on HDFS. The listening directory is
./split-dataset-monitor, which is also already on HDFS.

To solve question 1, run as follows:
~/software/spark-2.0.0-bin-hadoop2.6/bin/spark-submit ./PartAQuestion1.py ./split-dataset-monitor
./streaming-emulator.sh

To solve question 2, run as follows:
~/software/spark-2.0.0-bin-hadoop2.6/bin/spark-submit ./PartAQuestion2.py ./split-dataset-monitor ./Part-A-Qs2-output
./streaming-emulator.sh
Note the ./Part-A-Qs2-output is the directory where we store the output, thus there will be no output to the console. But
we show the result in the document.

To solve question 3, run as follows:
~/software/spark-2.0.0-bin-hadoop2.6/bin/spark-submit ./PartAQuestion3.py ./split-dataset-monitor ./input-list.csv
./streaming-emulator.sh

