import threading
import argparse
import logging
import json
import os

from py2neo import Graph, authenticate

import load2Neo4jDB

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('inputfile', metavar='FILE', help='Input file name', default="graph.json")
    parser.add_argument('--credentials', metavar='FILE', help='neo4j credentials file',
                        default=os.path.join(os.path.expanduser("~"), "credentials.json"))
    args = parser.parse_args()


    logger.info('Connecting to Neo4j and authenticating user credentials')
    with open(args.credentials) as creds:
        db_info = json.load(creds)
    authenticate(db_info['machine'], db_info['username'], db_info['password'])
    db_dir = db_info['machine'] + "/data"
    graph = Graph(db_dir)

    try:
        load2Neo4jDB.json2neo4j(str(json_file), graph)
    except:
        logging.error('Error involving loading data from json file to Neo4j database')
        raise
