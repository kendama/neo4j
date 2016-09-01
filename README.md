# Neo4j Graph Database for Synapse

Neo4j is noted as the “World’s Leading Graph Database”. A graph database allows data to be organized such that relationships are prioritize. Those relationships can be exploited through queries that strongly consider the nodes and the connections between them. Synapse provides a means of recording information related to provenance, thus enabling an informal way of crediting Synapse user for their work. By loading this information regarding into a Neo4j graph database, users are empowered with a comprehensive means of tracking and visualizing provenance. To begin using Neo4j, download Neo4j for free from https://neo4j.com/download/.

This repository contains 3 usable scripts and a requirement.txt file with a list of items to be installed using pip. To install these dependencies, be sure to have pip, and then use ‘pip install -r requirement.txt’. Users must have Neo4j installed on a local or remote machine with their login information contained in a json file as follows:

{
    "machine": “your-machine”,
    "username": “your-username”,
    "password": “your-password”
}

Users must also have an active Synapse account.

convertSynapse2Graph.py is a script to sparingly be used for uploading file entities and activities from all projects in Synapse. The output is a json file that can be uploaded to your local or remote Neo4j repository using load2Neo4j.py. This script is a modification of this gist: https://gist.github.com/larssono/9657a888f24e7a836806cda60f484048#file-convertactivites2graph-py

load2Neo4j.py is a script that takes the json file outputted from running convertActivities2Graph.py. The data contained in the json file is loaded to your Neo4j database.

usage: load2Neo4jDB.py [-h] json

Please input the name of json outfile to load graph data to Neo4j database

positional arguments:
  json        Input the name of pregenerated json file

optional arguments:
  -h, --help  show this help message and exit

activities2Graph.py is a wrapper that allows the user to input a synId or a list of synIds for any given project or projects; and sequentially retrieves information on all entities, activities, and their provenance, creates a json file containing this information, and then loads this data directly to your Neo4j database. 

usage: activities2Graph.py [-h] [--j json] [--p P] synId [synId ...]

Please input the [1] synapse ID or space-separated list of synapse ID and the
[2] name of json outfile to graph provenance

positional arguments:
  synId       Input the synapse ID or list of synapse IDs

optional arguments:
  -h, --help  show this help message and exit
  --j json    Input name of json outfile
  --p P       Specify the pool size for the multiprocessing module



## Useful Cypher queries for Neo4j Database


#### Return a list with the name of all activities
```
MATCH (n:Activity) RETURN n.name
```
#### Return a list with the name of all entities
```
MATCH (n:Entity) RETURN n.name
```
#### Return a count of all activities
```
MATCH (n:Activity) RETURN count(n)
```
#### Return a count of all activities
```
MATCH (n:Entity) RETURN count(n)
```
#### Return ratio of activities to entity
```
MATCH (n:Activity) WITH toFloat(count(n)) as num MATCH (m:Entity) RETURN num/count(m)
```
#### Use known annotation/property value to find a particular node
```
MATCH (n {annotationName:"VALUE"}) RETURN n
```
or
```
MATCH (n) WHERE annotationName = "VALUE" RETURN n
```
Example - display all activities, entities, and their relationships stemming from a given user
```
MATCH p = (n {createdBy: "#######"})<-[*]-(m) RETURN DISTINCT p, collect(m)
```

#### Who is generating the most activity
```
START n = node(*) 
MATCH (n)--(c) WHERE EXISTS(n.createdBy)
RETURN n.createdBy, count(*) as connections
ORDER BY connections DESC
LIMIT 10
```

#### Display all provenance leading back to origin
```
MATCH p = (n {synId:”#######”})<-[*]-(m) RETURN DISTINCT p
```

#### Display the top ten node of highest degree
```
START n = node(*) 
MATCH (n)--(c)
RETURN n.name, n.synId, count(*) as connections
ORDER BY connections DESC
LIMIT 10
```

#### Display the most used entity/activity
```
START n = node(*) 
MATCH (n)<--(c)
RETURN n.name, n.synId, count(*) as connections
ORDER BY connections DESC
LIMIT 10
```

#### Display the most dependent entity/activity
```
START n = node(*) 
MATCH (n)-- >(c)
RETURN n.name, n.synId, count(*) as connections
ORDER BY connections DESC
LIMIT 10
```

#### Poor man’s recommendation engine
```
MATCH (n {createdBy:"YourOriginalID"})-->(c)
MATCH (m)-->(c) WHERE NOT m.createdBy = "YourOriginalID"
WITH m.createdBy as cb
RETURN DISTINCT cb, count(cb) as friendship
ORDER by friendship DESC
LIMIT 10
```
##### Take top result (or another of your choice)
```
MATCH (a {createdBy:"YourOriginalID"})-->(m)
MATCH (b {createdBy: "YourChoiceID"})-->(n) WHERE NOT m=n
WITH a,n
MATCH p = shortestPath((a)--(n))
RETURN n.name, n.synId, length(p)
ORDER BY length(p) LIMIT 3
```
### More detailed examples:

#### Return a list of synIds for every file derived from PCBC protocols not involving Aravind Ramakrishnan as the originating scientist that possess the particular annotation "Diffname_short"
```
MATCH (protocol {fileType:"protocol", projectId:"1773109.0"}) WHERE NOT protocol.Originating_Scientist = "Aravind Ramakrishnan" AND EXISTS(protocol.Diffname_short) WITH collect(protocol) as prots
UNWIND prots as prot
   MATCH (n {synId:prot.synId})<-[*]-(m:Entity) WHERE NOT (m)<-[*]-() AND m.projectId = m.benefactorId AND exists(m.Originating_Lab)
   RETURN n.synId AS parent_id, count(distinct m.synId) AS derived_files
```

#### Return a list of all files within PsychEncode which similarly used BWA alignment tool downloaded from SourceForge, ordered alphanumerically by their synId 
```
MATCH (n {projectId:"4921369.0", fileType:"bam"})-[r]-(s)-[t]->(m {name:"http://sourceforge.net/projects/bio-bwa/files/bwa-0.6.2.tar.bz2/download"}) RETURN DISTINCT n.name, n.synId AS synId ORDER BY synId DESC
```


Feel free to add or suggest other queries that would be equally useful or simply interesting. Queries can also be written programmatically using the Py2neo client library. An example of this can be found here: https://github.com/Sage-Bionetworks/PsychENCODE/tree/master/dataReleases/aug2016
