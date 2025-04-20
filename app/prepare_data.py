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

# Initialize Spark in local mode
spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()

df = spark.read.parquet("/b.parquet")

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

df.foreach(create_doc)

df.write.csv("/index/data", sep = "\t")