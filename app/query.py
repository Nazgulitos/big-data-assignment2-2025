# #!/usr/bin/env python3
# from cassandra.cluster import Cluster
# from cassandra.query import SimpleStatement

# def query_tables():
#     # Connect to Cassandra
#     cluster = Cluster(['cassandra-server'])
#     session = cluster.connect('search_index')
    
#     # Query vocabulary table
#     print("\nVocabulary table (first 5 entries):")
#     rows = session.execute("SELECT * FROM vocabulary LIMIT 5")
#     for row in rows:
#         print(f"Term: {row.term}, DF: {row.document_frequency}, Total: {row.total_occurrences}")
    
#     # Query document_stats table
#     print("\nDocument stats (first 5 entries):")
#     rows = session.execute("SELECT * FROM document_stats LIMIT 5")
#     for row in rows:
#         print(f"Doc ID: {row.doc_id}, Title: {row.title}, Length: {row.doc_length}")
    
#     # Query inverted_index table
#     print("\nInverted index (first 5 entries):")
#     rows = session.execute("SELECT * FROM inverted_index LIMIT 5")
#     for row in rows:
#         print(f"Term: {row.term}, Doc ID: {row.doc_id}, TF: {row.term_frequency}")
    
#     # Query bm25_scores table
#     print("\nBM25 scores (first 5 entries):")
#     rows = session.execute("SELECT * FROM bm25_scores LIMIT 5")
#     for row in rows:
#         print(f"Term: {row.term}, Doc ID: {row.doc_id}, Score: {row.score}")
    
#     # Close connection
#     cluster.shutdown()

# if __name__ == "__main__":
#     query_tables() 