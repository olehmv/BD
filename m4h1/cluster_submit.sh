#!/bin/bash

USER=$1

if [ -z ${USER} ]; then
    echo "Please provide your HDFS user name"
    exit 1
fi

spark-submit --master yarn-client --driver-memory 2g --num-executors 3 --executor-memory 1g --conf spark.executor.cores=5  m4h1-assembly-1.jar /user/$USER/spark/bids /user/root/spark/motels /user/$USER/spark/exchange_rates /user/$USER/spark-core-output