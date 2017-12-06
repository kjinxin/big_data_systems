#!/bin/sh

echo "Start: " $(date +%s)

$SPARK_HOME/bin/spark-submit /home/ubuntu/PartC/PartCQuestion-3.py /soc-LiveJournal1.txt 10 16

echo "End: " $(date +%s)
