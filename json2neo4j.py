#This file must be contained in the import folder on the machine running
#Neo4j. Otherwise, the user must change the configurations setting in 
#conf/neo4j.conf
import json
import csv
import pandas as pd
import os.path
import tempfile
import logging
from py2neo import Graph, authenticate

# Preconstructed queries
aQuery = """
    USING PERIODIC COMMIT 1000
    LOAD CSV WITH HEADERS FROM "{0}" AS dvs
    WITH dvs WHERE NOT dvs.concreteType = "activity"
       MERGE (entity:Entity {{id:dvs._id}}) ON CREATE
       SET entity = dvs
"""
bQuery = """
    USING PERIODIC COMMIT 1000
    LOAD CSV WITH HEADERS FROM "{0}" AS dvs
    WITH dvs WHERE dvs.concreteType = "activity"
       MERGE (activity:Activity {{id:dvs._id}}) ON CREATE
       SET activity = dvs
"""
cQuery = """
    USING PERIODIC COMMIT 1000
    LOAD CSV WITH HEADERS FROM "{0}" AS erow
    MATCH (in_node {{ _id:erow._inV }})
    MATCH (out_node {{ _id:erow._outV }})
    MERGE (out_node)<-[:USED {{ action:erow._label, id:erow._id }}]-(in_node)
"""

def json2neo4j(jsonfilename):
    graph = Graph()
    global aQuery, bQuery, cQuery
    # Retrieve JSON/CSV file
    print 'Creating temporary JSON/CSV file'
    logging.info('Creating temporary JSON/CSV file')
    nodes = tempfile.NamedTemporaryFile(prefix='vertices', suffix='.csv')
    edges = tempfile.NamedTemporaryFile(prefix='edges', suffix='.csv')
    
    logging.info('Converting JSON to CSV')
    with open(jsonfilename) as json_file:
        JSON = json.load(json_file)

    df1 = pd.DataFrame(JSON['vertices'])
    df1 = df1.drop('used', 1)
    df1.to_csv(nodes.name, index=False)

    df2 = pd.DataFrame(JSON['edges'])
    df2.to_csv(edges.name, index=False) 
    # index=False removes first column with null header

    # Add uniqueness constraints and indexing
    print 'Establishing uniqueness constraints and indexing'
    logging.info('Establishing uniqueness constraints and indexing for Neo4j')
    graph.run("CREATE CONSTRAINT ON (entity:Entity) ASSERT entity.id IS UNIQUE")
    graph.run("CREATE CONSTRAINT ON (activity:Activity) ASSERT activity.id IS UNIQUE")
    graph.run("CREATE INDEX ON :Entity(entity)")

    # Build query
    aQuery = aQuery.format(nodes.name)
    bQuery = bQuery.format(nodes.name)
    cQuery = cQuery.format(edges.name)

    # Send Cypher query
    print('Loading data from CSV file(s) to Neo4j')
    logging.info('Loading data from CSV file(s) to Neo4j')
    graph.run(aQuery)
    graph.run(bQuery)
    graph.run(cQuery)
    graph.run("DROP CONSTRAINT ON (entity:Entity) ASSERT entity.id IS UNIQUE")
    graph.run("DROP CONSTRAINT ON (activity:Activity) ASSERT activity.id IS UNIQUE")
    graph.run("MATCH (n) WHERE n:Activity OR n:Entity REMOVE n.id").evaluate()
    print('Done.')
    logging.info('Done.')

    # Clean up directory and remove created files
    # Comment out if you would like to keep csv files
    print('Removing csv files from local directory')
    nodes.close()
    edges.close()

