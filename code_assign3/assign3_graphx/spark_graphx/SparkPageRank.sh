#!/bin/sh

echo "Start: " $(date +%s)

$SPARK_HOME/bin/spark-submit --class SparkPageRank target/spark_graphx-1.0.jar /web-BerkStan.txt 20

echo "End: " $(date +%s)
