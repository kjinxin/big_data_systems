#!/bin/sh

echo "Start: " $(date +%s)

$SPARK_HOME/bin/spark-submit --class PartBApplication1Question1 /home/ubuntu/assign3_graphx/spark_graphx/target/spark_graphx-1.0.jar /soc-LiveJournal1.txt 20

echo "End: " $(date +%s)
