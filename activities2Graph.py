import synapseclient
import load2Neo4jDB as ndb
import convertSynapse2Graph
import multiprocessing.dummy as mp
import threading
import argparse
import logging
import json
import tempfile
import sys

from collections import OrderedDict
from py2neo import Graph, authenticate

syn = synapseclient.login()

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    import os


    parser = argparse.ArgumentParser(description=
                '''Please input [1] the synapse ID or space-separated list of synapse ID and
                            [2, default: graph.json] the name of json outfile to graph provenance and
                            [3, default: # of available cores] the mp pool size''')
    parser.add_argument('id', metavar='synId', nargs='+', help='Input the synapse ID or list of synapse IDs')
    parser.add_argument('-n', type=int, help='Specify the pool size for the multiprocessing module', default=2)
    parser.add_argument('-l', '--load', action='store_true', default=False, help='Load data from json file to Neo4j database')
    args = parser.parse_args()

    p = mp.Pool(args.n)

    nodes = dict()

    if args.load:
        with open(os.path.join(os.path.expanduser("~"), "credentials.json")) as creds:
            db_info = json.load(creds)
            logger.info("Loaded neo4j credentials.")

    for proj in args.id:
        logger.info('Getting entities from %s' %proj)
        nodes.update(convertSynapse2Graph.processEntities(projectId = proj))

    logger.info('Fetched %i entities' % len(nodes))

    activities = p.map(convertSynapse2Graph.safeGetActivity, nodes.items())
    activities = convertSynapse2Graph.cleanUpActivities(activities)

    if len(activities) > 0:
        print '%i activities found i.e. %f%% entities have provenance' %(len(activities),
                                                                            float(len(nodes))/len(activities))
    else:
        print 'This project lacks accessible information on provenance'

    edges = convertSynapse2Graph.buildEdgesfromActivities(nodes, activities)
    logger.info('I have  %i nodes and %i edges' %(len(nodes), len(edges)))

    if args.load:
        tmpfile = tempfile.NamedTemporaryFile()
        with tempfile.NamedTemporaryFile(suffix=".json") as fp:
            json.dump(OrderedDict([('vertices', map(dict, nodes.values())), ('edges', edges)]), fp, indent=4)

            logger.info('Connecting to Neo4j and authenticating user credentials')
            authenticate(db_info['machine'], db_info['username'], db_info['password'])
            db_dir = db_info['machine'] + "/db/data"
            graph = Graph(db_dir)

            ndb.json2neo4j(fp.name, graph)
    else:
        with sys.stdout as fp:
            json.dump(OrderedDict([('vertices', map(dict, nodes.values())), ('edges', edges)]), fp, indent=4)
