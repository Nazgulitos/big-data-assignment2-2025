# Default input path in HDFS
STAGE_1_INPUT="/index/data/part-*"

# Create directories if they don't exist
hdfs dfs -mkdir -p /tmp/index/input
hdfs dfs -mkdir -p /tmp/index/stage1
hdfs dfs -mkdir -p /tmp/index/stage2

# List input directory
echo "Checking input directory:"
hdfs dfs -ls /tmp/index/input

# Stage 1
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
  -archives /app/.venv.tar.gz#.venv \
  -D mapreduce.framework.name=yarn \
  -mapper ".venv/bin/python mapper1.py" \
  -reducer ".venv/bin/python reducer1.py 2> reducer.log" \
  -input "$STAGE_1_INPUT" \
  -output /tmp/index/stage1 \
  -numReduceTasks 1

# Check stage 1 output
hdfs dfs -ls /tmp/index/stage1
hdfs dfs -cat /tmp/index/stage1/part-*

# Stage 2 
echo "Stage 2"
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files /app/mapreduce/mapper2.py,/app/mapreduce/reducer2.py \
  -archives /app/.venv.tar.gz#.venv \
  -D mapreduce.framework.name=yarn \
  -mapper ".venv/bin/python mapper2.py" \
  -reducer ".venv/bin/python reducer2.py" \
  -input /tmp/index/stage1/part-* \
  -output /tmp/index/stage2 \
  -numReduceTasks 1

# Check final output
hdfs dfs -ls /tmp/index/stage2
hdfs dfs -cat /tmp/index/stage2/part-*

echo "Indexing completed successfully!"
