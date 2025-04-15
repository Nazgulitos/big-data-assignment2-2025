#!/usr/bin/env python3
import sys
import math

def main():
    # Initialize document statistics
    total_docs = 0
    total_length = 0
    doc_lengths = {}
    
    # First pass: collect document statistics
    for line in sys.stdin:
        parts = line.strip().split('\t')
        if parts[0] == "DOC":
            doc_id, _, length, _ = parts[1:]
            doc_lengths[doc_id] = int(length)
            total_docs += 1
            total_length += int(length)
    
    # Calculate average document length
    avg_doc_length = total_length / total_docs if total_docs > 0 else 0
    
    # Second pass: calculate and emit BM25 scores
    for line in sys.stdin:
        parts = line.strip().split('\t')
        if parts[0] == "INDEX":
            term, doc_id, freq, _ = parts[1:]
            doc_length = doc_lengths.get(doc_id, 0)
            
            # Calculate BM25 score
            k1 = 1.2
            b = 0.75
            idf = math.log((total_docs - 1 + 0.5) / (1 + 0.5))  # Simplified IDF
            tf = int(freq)
            score = idf * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (doc_length / avg_doc_length)))
            
            # Emit BM25 score
            print(f"{term}\t{doc_id}\t{score}")

if __name__ == "__main__":
    main() 