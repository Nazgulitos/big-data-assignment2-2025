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

# Package the virtual env.
venv-pack -o .venv.tar.gz

# Building Cassandra

if [[ ! -f cassandra_lib.zip ]]; then
    mkdir -p cassandra_lib
    pip install cassandra-driver -t cassandra_lib
    (cd cassandra_lib && zip -qr ../cassandra_lib.zip .)
    rm -rf cassandra_lib
fi

echo "Building Cassandra"
python app.py

# Collect Data
echo "Collecting data"
bash prepare_data.sh

# Run the indexer
echo "Indexing data"
bash index.sh

# # Run the ranker
# echo "Running the ranker"
# bash search.sh "The ancient world"