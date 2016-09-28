# Neo4j Graph Database for Synapse

Synapse provides a means of recording information related to provenance as a graph, thus enabling an formal way of crediting Synapse user for their work. However, the current implementation does not provide easily accessible mechanisms to search or discover structures in the provenance graph across different activities. This repository provides the mechanisms for loading Synapse provenance information into a graph database, which allows data to be organized such that relationships are prioritized. Those relationships can be exploited through queries that consider the nodes and the connections between them. By loading this information regarding into a graph database, users are empowered with a flexible means of tracking, searching, and visualizing provenance.

Here, we use the [Neo4j](https://neo4j.com/) graph database. To begin using Neo4j on your local machine, download Neo4j for free from https://neo4j.com/download/, follow their online [instructions](https://neo4j.com/developer/guide-neo4j-browser/) to access the Neo4j browser, and use the scripts in this repository to load data from any Synapse project to your graph database. Sage Bionetwork has also launched an instance of Neo4j on Amazon EC2. The EC2 instance type is an m3.xlarge model with 15 GB of RAM and 2 x 40 GB of SSD storage memory. For more information on the EC2 instance, contact x.schildwachter@sagebase.org. For all other questions, suggestions, or inquiries, please contact Kennedy Agwamba at kennedy.agwamba@sagebase.org.


This repository contains 3 usable scripts and a requirement.txt file with a list of items to be installed using pip. To install these dependencies, be sure to have pip, and then use ‘pip install -r requirement.txt’. Users must have Neo4j installed on a local or remote machine with their login information contained in a json file as follows:
```
{
    "machine": “your-machine”,
    "username": “your-username”,
    "password": “your-password”
}
```
Users must also have an active Synapse account.

convertSynapse2Graph.py is a script to sparingly be used for uploading file entities and activities from all projects in Synapse. The output is a json file that can be uploaded to your local or remote Neo4j repository using load2Neo4j.py. This script is a modification of this [gist](https://gist.github.com/larssono/9657a888f24e7a836806cda60f484048#file-convertactivites2graph-py).
```
usage: convertSynapse2Graph.py [-h] [--p P] [--j json] [--l L]

Creates a json file based on provenance for all Synapse projects

optional arguments:
  -h, --help  show this help message and exit
  --p P       Specify the pool size for the multiprocessing module
  --j json    Input name of json outfile
  --l L       Load data from json file to Neo4j database
```
load2Neo4j.py is a script that takes the json file outputted from running convertActivities2Graph.py. The data contained in the json file is loaded to your Neo4j database.
```
usage: load2Neo4jDB.py [-h] json

Please input the name of json outfile to load graph data to Neo4j database

positional arguments:
  json        Input the name of pregenerated json file

optional arguments:
  -h, --help  show this help message and exit
```
activities2Graph.py is a wrapper that allows the user to input a synId or a list of synIds for any given project or projects; and sequentially retrieves information on all entities, activities, and their provenance, creates a json file containing this information, and then loads this data directly to your Neo4j database. 
```
usage: activities2Graph.py [-h] [--j json] [--p P] [--l L] synId [synId ...]

Please input [1] the synapse ID or space-separated list of synapse ID and [2,
default: graph.json] the name of json outfile to graph provenance and [3,
default: # of available cores] the mp pool size

positional arguments:
  synId       Input the synapse ID or list of synapse IDs

optional arguments:
  -h, --help  show this help message and exit
  --j json    Input name of json outfile
  --p P       Specify the pool size for the multiprocessing module
  --l L       Load data from json file to Neo4j database
```

See examples of useful cypher queries [here](https://github.com/Sage-Bionetworks/synapseGraphDB-neo4j/blob/master/examples/cypher_queries.md).
