cd ~
source run.sh
number=0
while [ "$number" -lt 1127 ]; do
    number=$((number + 1))
    hadoop fs -mv ./split-dataset/${number}.csv ./split-dataset-monitor
    sleep 10
done
hadoop fs -mv /split-dataset-monitor/* ./split-dataset/
