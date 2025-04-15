#!/bin/bash

echo "Running prepare_data.sh"

# Активируем venv
source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

ZIPPED_PARQUET_FILE="b.parquet.zip"
PARQUET_FILE="b.parquet"

# Распаковка
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

# Очистка и пересоздание локальной папки
rm -rf data index
mkdir data index

# Загружаем parquet в HDFS в папку /app
hdfs dfs -mkdir -p /app
hdfs dfs -put -f b.parquet /app/

# Запускаем Spark
spark-submit prepare_data.py

# Загружаем результат в HDFS
echo "Putting data to HDFS..."
hdfs dfs -rm -r -skipTrash /app/data 2>/dev/null
hdfs dfs -rm -r -skipTrash /app/index 2>/dev/null
hdfs dfs -put data /app/
hdfs dfs -put index /app/

echo "Done with data preparation!"