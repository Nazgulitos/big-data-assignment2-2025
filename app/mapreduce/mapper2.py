#!/usr/bin/env python3
import sys

def main():
    for line in sys.stdin:
        # Parse input line (term\tdoc_id\tfreq\tpositions)
        term, doc_id, freq, positions = line.strip().split('\t')
        
        # Emit term and document information
        print(f"{term}\t{doc_id}\t{freq}\t{positions}")

if __name__ == "__main__":
    main() 