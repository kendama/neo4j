import multiprocessing
import threading
import argparse
import logging
import json
import tempfile
import itertools
import sys
from collections import OrderedDict

import synapseclient

import synapsegraph

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    import os

    parser = argparse.ArgumentParser(description='Convert Synapse projects to JSON for import into neo4j graph database.')
    parser.add_argument('id', metavar='synId', nargs='*',
                        help='Input the synapse ID or list of synapse IDs. If no IDs given, all available Synapse projects will be used.')
    parser.add_argument('-n', type=int,
                        help='Specify the pool size for the multiprocessing module',
                        default=2)
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Turn on debugging output.')
    args = parser.parse_args()

    syn = synapseclient.login(silent=True)

    pool = multiprocessing.dummy.Pool(args.n)

    nodes = dict()

    if args.debug:
        logger.setLevel(logging.DEBUG)
        synapsegraph.logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
        synapsegraph.logger.setLevel(logging.INFO)

    if not args.id:
        logger.warn("No Project ids given, getting all projects from Synapse.")
        q = syn.chunkedQuery('select id from project')
        args.id = itertools.imap(lambda x: x['project.id'], q)
    for proj in args.id:
        if proj in synapsegraph.SKIP_LIST:
            logger.info("Skipping %s" % proj)
            continue
        else:
            logger.info('Processing Project %s' %proj)
            nodes.update(synapsegraph.processEntities(projectId = proj, pool=pool))

    logger.info('Fetched %i entities' % len(nodes))

    activities = pool.map(synapsegraph.safeGetActivity, nodes.items())
    activities = synapsegraph.cleanUpActivities(activities)

    if len(activities) > 0:
        logger.info('%i activities found i.e. %f%% entities have provenance' %(len(activities),
                                                                            float(len(nodes))/len(activities)))
    else:
        print 'This project lacks accessible information on provenance'

    edges = synapsegraph.buildEdgesfromActivities(nodes, activities)
    logger.info('I have  %i nodes and %i edges' %(len(nodes), len(edges)))

    with sys.stdout as fp:
        json.dump(OrderedDict([('vertices', map(dict, nodes.values())), ('edges', edges)]), fp, indent=4)
