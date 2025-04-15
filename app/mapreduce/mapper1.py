#!/usr/bin/env python3
import sys
import re

def tokenize(text):
    # Convert to lowercase and split into words
    words = re.findall(r'\w+', text.lower())
    return words

def main():
    for line in sys.stdin:
        # Parse input line (id\ttitle\tcontent)
        doc_id, title, content = line.strip().split('\t', 2)
        
        # Tokenize content
        tokens = tokenize(content)
        
        # Calculate document length
        doc_length = len(tokens)
        
        # Calculate term frequencies and positions
        term_freq = {}
        term_positions = {}
        for pos, term in enumerate(tokens):
            if term not in term_freq:
                term_freq[term] = 0
                term_positions[term] = []
            term_freq[term] += 1
            term_positions[term].append(pos)
        
        # Emit document statistics
        print(f"STATS\t{doc_id}\t{title}\t{doc_length}\t{sum(term_freq.values()) / len(term_freq) if term_freq else 0}")
        
        # Emit term frequencies and positions
        for term, freq in term_freq.items():
            positions = term_positions[term]
            print(f"{term}\t{doc_id}\t{freq}\t{','.join(map(str, positions))}")

if __name__ == "__main__":
    main()