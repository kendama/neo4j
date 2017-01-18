#This file must be contained in the import folder on the machine running
#Neo4j. Otherwise, the user must change the configurations setting in
#conf/neo4j.conf
import json
import csv
import pandas as pd
import os
import sys
import logging
import argparse
import tempfile
import py2neo

# Preconstructed queries
entityNodeQuery = """
    USING PERIODIC COMMIT 1000
    LOAD CSV WITH HEADERS FROM "file://%s" AS dvs
    WITH dvs WHERE NOT dvs.concreteType = "activity"
       MERGE (entity:Entity {id:dvs._id}) ON CREATE
       SET entity = dvs
"""
activityNodeQuery = """
    USING PERIODIC COMMIT 1000
    LOAD CSV WITH HEADERS FROM "file://%s" AS dvs
    WITH dvs WHERE dvs.concreteType = "activity"
       MERGE (activity:Activity {id:dvs._id}) ON CREATE
       SET activity = dvs
"""
generatedByEdgeQuery = """
    USING PERIODIC COMMIT 1000
    LOAD CSV WITH HEADERS FROM "file://%s" AS erow
    MATCH (in_node:Entity { _id:erow._inV })
    MATCH (out_node:Activity { _id:erow._outV })
    MERGE (out_node)-[:GENERATED_BY { action:erow._label, id:erow._id }]->(in_node)
"""
usedEdgeQuery = """
    USING PERIODIC COMMIT 1000
    LOAD CSV WITH HEADERS FROM "file://%s" AS erow
    MATCH (in_node:Activity { _id:erow._inV })
    MATCH (out_node:Entity { _id:erow._outV })
    MERGE (out_node)-[:USED { action:erow._label, id:erow._id }]->(in_node)
"""

nodeQueries = [entityNodeQuery, activityNodeQuery]

edgeQueries = [generatedByEdgeQuery, usedEdgeQuery]

def nodes2neo(datafile, graph):
    # # Change file permission and ownership for Neo4j
    # os.chown(nodes.name, uid, gid)
    #
    # Add uniqueness constraints and indexing
    graph.run("CREATE CONSTRAINT ON (entity:Entity) ASSERT entity.id IS UNIQUE")
    graph.run("CREATE INDEX ON :Entity(entity)")
    nodeQuery = entityNodeQuery % nodes.name

    logging.info('node query: %s' % (nodeQuery, ))

    #Send Cypher query
    logging.info('Loading data from CSV file(s) to Neo4j')
    graph.run(nodeQuery)

    graph.run("DROP CONSTRAINT ON (entity:Entity) ASSERT entity.id IS UNIQUE")
    graph.run("MATCH (n) WHERE n:Entity REMOVE n.id").evaluate()
    logging.info('Done.')

def json2neo4j(data, graph, node_queries = nodeQueries, edge_queries = edgeQueries):
    # Retrieve JSON/CSV file
    logging.info('Creating temporary JSON/CSV file')
    nodes = tempfile.NamedTemporaryFile(prefix='vertices', suffix='.csv', delete=False)
    edges = tempfile.NamedTemporaryFile(prefix='edges', suffix='.csv', delete=False)

    dir_info = os.stat('.')
    uid = dir_info.st_uid
    gid = dir_info.st_gid

    logging.info('Converting JSON to CSV')

    df1 = pd.DataFrame(data['vertices'])
    if 'used' in df1.columns:
        df1 = df1.drop('used', 1)

    if 'description' in df1.columns:
        df1 = df1.drop('description', 1)

    df1.to_csv(nodes.name, index=False)

    df2 = pd.DataFrame(data['edges'])

    if df2.empty:
        print 'No edges/activities/provenance in graph'

        # # Change file permission and ownership for Neo4j
        # os.chown(nodes.name, uid, gid)
        #
        # # Add uniqueness constraints and indexing
        # graph.run("CREATE CONSTRAINT ON (entity:Entity) ASSERT entity.id IS UNIQUE")
        # graph.run("CREATE INDEX ON :Entity(entity)")
        # for nodeQuery in node_queries:
        #     nodeQuery = nodeQuery % nodes.name
        #
        # #Send Cypher query
        # logging.info('Loading data from CSV file(s) to Neo4j')
        # graph.run(nodeQuery[0])
        #
        # graph.run("DROP CONSTRAINT ON (entity:Entity) ASSERT entity.id IS UNIQUE")
        # graph.run("MATCH (n) WHERE n:Entity REMOVE n.id").evaluate()
        # logging.info('Done.')
        #
        # # Clean up directory and remove created files
        # # Comment out if you would like to keep csv files
        # print('Removing csv files from local directory')
        # nodes.close()

    else:
        df2.to_csv(edges.name, index=False)

        # # Change file permission and ownership for Neo4j
        # os.chown(nodes.name, uid, gid)
        # os.chown(edges.name, uid, gid)
        #
        # # Add uniqueness constraints and indexing
        # logging.info('Establishing uniqueness constraints and indexing for Neo4j')
        # graph.run("CREATE CONSTRAINT ON (entity:Entity) ASSERT entity.id IS UNIQUE")
        # graph.run("CREATE CONSTRAINT ON (activity:Activity) ASSERT activity.id IS UNIQUE")
        # graph.run("CREATE INDEX ON :Entity(entity)")
        #
        # # Build query
        # logging.info('Loading data from CSV file(s) to Neo4j')
        # for nodeQuery in node_queries:
        #     nodeQuery = nodeQuery % nodes.name
        #     graph.run(nodeQuery)
        #
        # for edgeQuery in edge_queries:
        #     edgeQuery = edgeQuery % edges.name
        #     graph.run(edgeQuery)
        #
        # # Send Cypher query
        # graph.run("DROP CONSTRAINT ON (entity:Entity) ASSERT entity.id IS UNIQUE")
        # graph.run("DROP CONSTRAINT ON (activity:Activity) ASSERT activity.id IS UNIQUE")
        # graph.run("MATCH (n) WHERE n:Activity OR n:Entity REMOVE n.id").evaluate()
        # logging.info('Done.')
        #
        # # Clean up directory and remove created files
        # # Comment out if you would like to keep csv files
        # print('Removing csv files from local directory')
        # nodes.close()
        # edges.close()

    return nodes, edges

if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    parser = argparse.ArgumentParser(description='Load graph data to Neo4j database.')
    parser.add_argument('jsonfile', metavar='json', help='Input the name of pregenerated json file')
    args = parser.parse_args()

    # Connect to graph
    print 'Connecting to Neo4j and authenticating user credentials'
    with open('/home/kdaily/credentials.json') as json_file:
        db_info=json.load(json_file)

    # authenticate(db_info['machine'], db_info['username'], db_info['password'])
    # db_dir = db_info['machine'] + "/db/data"
    graph = py2neo.Graph(host=db_info['machine'], password=db_info['password'])

    try:
        with open(args.jsonfile) as json_file:
            data = json.load(json_file)

        nodes, edges = json2neo4j(data, graph)
        nodes2neo(nodes, graph)
    except:
        print 'Error involving loading data from json file to Neo4j database'
        raise
