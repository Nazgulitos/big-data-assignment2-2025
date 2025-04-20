#!/bin/bash

# Ensure cassandra_lib.zip exists
if [[ ! -f cassandra_lib.zip ]]; then
    mkdir -p cassandra_lib
    pip install cassandra-driver -t cassandra_lib
    (cd cassandra_lib && zip -qr ../cassandra_lib.zip .)
    rm -rf cassandra_lib
fi

# Activate the virtual environment
source .venv/bin/activate

# Remove any existing HDFS output directory
hdfs dfs -rm -r -f /tmp/index || true

# Stage 1: Run the first MapReduce job
echo "Stage 1: Running MapReduce job for term frequency calculation"
hadoop jar $(find /usr/lib /opt /usr/local -name hadoop-streaming*.jar 2>/dev/null | head -n 1) \
    -input ${1:-/index/data} \
    -output /tmp/index \
    -mapper mapreduce/mapper1.py \
    -reducer mapreduce/reducer1.py \
    -file mapreduce/mapper1.py \
    -file mapreduce/reducer1.py \
    -file cassandra_lib.zip

# Check Stage 1 output
echo "Checking Stage 1 output..."
hdfs dfs -ls /tmp/index
hdfs dfs -cat /tmp/index/part-00000 | head -n 10

# Stage 2: Run the second MapReduce job for document frequency calculation
echo "Stage 2: Running MapReduce job for document frequency calculation"
hdfs dfs -rm -r -f /tmp/index_stage2 || true
hadoop jar $(find /usr/lib /opt /usr/local -name hadoop-streaming*.jar 2>/dev/null | head -n 1) \
    -input /tmp/index \
    -output /tmp/index_stage2 \
    -mapper mapreduce/mapper2.py \
    -reducer mapreduce/reducer2.py \
    -file mapreduce/mapper2.py \
    -file mapreduce/reducer2.py \
    -file cassandra_lib.zip

# Check Stage 2 output
echo "Checking Stage 2 output..."
hdfs dfs -ls /tmp/index_stage2
hdfs dfs -cat /tmp/index_stage2/part-00000 | head -n 10

# Final Step: Insert data into Cassandra
echo "Inserting data into Cassandra..."
python3 app.py

# Verify final output in Cassandra
echo "Verifying data in Cassandra..."
cqlsh -e "USE search_index; SELECT * FROM vocabulary LIMIT 10;"
cqlsh -e "USE search_index; SELECT * FROM document_index LIMIT 10;"

echo "Indexing completed successfully!"