#!/bin/sh

echo "Start: " $(date +%s)

$SPARK_HOME/bin/spark-submit --class PartBApplication2Question2 /home/ubuntu/assign3_graphx/spark_graphx/target/spark_graphx-1.0.jar /twitter_graph.txt

echo "End: " $(date +%s)
