# #!/bin/bash
# Start ssh server
service ssh restart 

# echo "Starting the services"
bash start-services.sh

# echo "Creating a virtual environment"
python3 -m venv .venv
source .venv/bin/activate

# echo "Installing packages"
pip install -r requirements.txt  

# # Package the virtual env.
venv-pack -o .venv.tar.gz

# # Building Cassandra
echo "Building Cassandra"
python app.py

# Collect Data
echo "Collecting data"
bash prepare_data.sh

# Run the indexer
echo "Indexing data"
bash index.sh data/sample.txt

# # Run the ranker
# echo "Running the ranker"
# bash search.sh "this is a query!"