#!/usr/bin/env python3
from pathvalidate import sanitize_filename
from tqdm import tqdm
from pyspark.sql import SparkSession
import os
import re
import subprocess

print("Starting data preparation...")

# Create local directory for text files
os.makedirs("data", exist_ok=True)

# Initialize Spark without local mode
spark = SparkSession.builder \
    .appName('data preparation') \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()

try:
    # Check if input file exists
    input_path = "/app/b.parquet"
    print(f"Checking for input file at: {input_path}")
    
    if not os.path.exists(input_path):
        print(f"Error: File not found at {input_path}")
        print("Current directory contents:")
        os.system("ls -l /app")
        exit(1)
    
    print(f"Reading parquet file from: {input_path}")
    try:
        # Use file:// prefix for local file
        df = spark.read.parquet(f"file://{input_path}")
        print("Successfully read parquet file")
        print(f"Number of rows: {df.count()}")
        print(f"Schema: {df.schema}")
    except Exception as e:
        print(f"Error reading parquet file: {str(e)}")
        print("Detailed error information:")
        import traceback
        traceback.print_exc()
        exit(1)
    
    # Process a sample of the data
    n = 1000
    print(f"Processing {n} documents...")
    df = df.select(['id', 'title', 'text']).sample(fraction=100 * n / df.count(), seed=0).limit(n)

    def is_plain_text(text):

        return not re.match(r"^\s*(<\?xml|<\w+>|{|\[)", text)

    def create_doc(row):
        text = row['text']
        if text and text.strip() and is_plain_text(text):
            filename = "data/" + sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
            with open(filename, "w", encoding="utf-8") as f:
                f.write(text)

    # Process the data and create text files locally
    print("Processing documents...")
    df.foreach(create_doc)
    
    # Create HDFS directory structure
    print("Setting up HDFS directories...")
    subprocess.run("hdfs dfs -mkdir -p /tmp/index/input", shell=True, check=True)
    subprocess.run("hdfs dfs -rm -r -skipTrash /tmp/index/input/*", shell=True, check=False)
    
    # Copy files to HDFS
    print("Putting data to HDFS...")
    subprocess.run("hdfs dfs -put -f data/* /tmp/index/input/", shell=True, check=True)
    
    # Verify files in HDFS
    print("Files in HDFS:")
    subprocess.run("hdfs dfs -ls /tmp/index/input", shell=True, check=True)
    
    print("Done with data preparation!")

except Exception as e:
    print(f"Error: {str(e)}")
    import traceback
    traceback.print_exc()

finally:
    spark.stop()