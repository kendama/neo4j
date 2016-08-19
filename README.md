# Neo4j Graph Database for Synapse

## Useful Cypher queries for Neo4j Database

### Return a list with the name of all activities

MATCH (n:Activity) RETURN n.name


### Return a list with the name of all entities

MATCH (n:Entity) RETURN n.name


### Return a count of all activities

MATCH (n:Activity) RETURN count(n)


### Return a count of all activities

MATCH (n:Entity) RETURN count(n)


### Return ratio of activities to entity

MATCH (n:Activity) WITH toFloat(count(n)) as num MATCH (m:Entity) RETURN num/count(m)


### Use known annotation/property value to find a particular node

MATCH (n {annotationName:"VALUE"}) RETURN n

or

MATCH (n) WHERE annotationName = "VALUE" RETURN n

Example - display all activities, entities, and their relationships stemming from a given user

MATCH p = (n {createdBy: "#######"})<-[*]-(m) RETURN DISTINCT p, collect(m)


### Who is generating the most activity

START n = node(*) 
MATCH (n)--(c) WHERE EXISTS(n.createdBy)
RETURN n.createdBy, count(*) as connections
ORDER BY connections DESC
LIMIT 10


### Display all provenance leading back to origin

MATCH p = (n {synId:”#######”})<-[*]-(m) RETURN DISTINCT p


### Display the top ten node of highest degree

START n = node(*) 
MATCH (n)--(c)
RETURN n.name, n.synId, count(*) as connections
ORDER BY connections DESC
LIMIT 10


### Display the most used entity/activity

START n = node(*) 
MATCH (n)<--(c)
RETURN n.name, n.synId, count(*) as connections
ORDER BY connections DESC
LIMIT 10


### Display the most dependent entity/activity

START n = node(*) 
MATCH (n)-- >(c)
RETURN n.name, n.synId, count(*) as connections
ORDER BY connections DESC
LIMIT 10


### Poor man’s recommendation engine
MATCH (n {createdBy:"YourOriginalID"})-->(c)
MATCH (m)-->(c) WHERE NOT m.createdBy = "YourOriginalID"
WITH m.createdBy as cb
RETURN DISTINCT cb, count(cb) as friendship
ORDER by friendship DESC
LIMIT 10
##### Take top result (or another of your choice)
MATCH (a {createdBy:"YourOriginalID"})-->(m)
MATCH (b {createdBy: "YourChoiceID"})-->(n) WHERE NOT m=n
WITH a,n
MATCH p = shortestPath((a)--(n))
RETURN n.name, n.synId, length(p)
ORDER BY length(p) LIMIT 3


### More detailed examples:

#### Return a list of synIds for every file derived from PCBC protocols not involving Aravind Ramakrishnan as the originating scientist that possess the particular annotation "Diffname_short"

MATCH (protocol {fileType:"protocol", projectId:"1773109.0"}) WHERE NOT protocol.Originating_Scientist = "Aravind Ramakrishnan" AND EXISTS(protocol.Diffname_short) WITH collect(protocol) as prots
UNWIND prots as prot
   MATCH (n {synId:prot.synId})<-[*]-(m:Entity) WHERE NOT (m)<-[*]-() AND m.projectId = m.benefactorId AND exists(m.Originating_Lab)
   RETURN n.synId AS parent_id, count(distinct m.synId) AS derived_files


#### Return a list of all files within PsychEncode which similarly used BWA alignment tool downloaded from SourceForge, ordered alphanumerically by their synId 

MATCH (n {projectId:"4921369.0", fileType:"bam"})-[r]-(s)-[t]->(m {name:"http://sourceforge.net/projects/bio-bwa/files/bwa-0.6.2.tar.bz2/download"}) RETURN DISTINCT n.name, n.synId AS synId ORDER BY synId DESC
