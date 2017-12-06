#!/bin/sh

echo "Start: " $(date +%s)

for i in 2 4 6 8 16 32 64 100 128 200 256 300
do
(time $SPARK_HOME/bin/spark-submit PartCQuestion-1.py ../web-BerkStan.txt 10 ${i}) 2> output/partc_1.out
done
echo "End: " $(date +%s)
