#!/bin/bash

echo "Running prepare_data.sh"

# Activate venv
source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

ZIPPED_PARQUET_FILE="b.parquet.zip"
PARQUET_FILE="b.parquet"

# Open
if [ ! -f "$PARQUET_FILE" ]; then
    if [ -f "$ZIPPED_PARQUET_FILE" ]; then
        echo "Unzipping $ZIPPED_PARQUET_FILE"
        unzip "$ZIPPED_PARQUET_FILE"
    else
        echo "Neither $PARQUET_FILE nor $ZIPPED_PARQUET_FILE found. Exiting."
        exit 1
    fi
else
    echo "$PARQUET_FILE was found"
fi

# Put data from parquet file
hdfs dfs -put -f b.parquet / && \
    spark-submit prepare_data.py && \
    echo "Putting data to hdfs" && \
    hdfs dfs -put data / && \
    hdfs dfs -ls /data && \
    hdfs dfs -ls /index/data && \
    echo "done data preparation!"

