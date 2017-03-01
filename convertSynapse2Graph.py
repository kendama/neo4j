import threading
import argparse
import logging
import json
import sys
from collections import OrderedDict
import multiprocessing
import UserDict

import synapseclient
import synapseutils

import load2Neo4jDB as ndb

syn = synapseclient.login(silent=True)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

NODETYPES = {0:'dataset',1: 'layer',2: 'project',3: 'preview',4: 'folder',
             5: 'analysis',6: 'step', 7: 'code',8: 'link',9: 'phenotypedata',
             10:'genotypedata',11:'expressiondata',12:'robject',
             13:'summary',14:'genomicdata',15:'page',16:'file',17:'table',
             18:'community'}

IGNOREME_NODETYPES = ['org.sagebionetworks.repo.model.Project',
                      'org.sagebionetworks.repo.model.Preview']

SKIP_LIST = ['syn582072', 'syn3218329', 'syn2044761', 'syn2351328', 'syn1450028']


class threadsafe_iter:
    """Takes an iterator/generator and makes it thread-safe by
    serializing call to the `next` method of given iterator/generator.
    """
    def __init__(self, it):
        self.it = it
        self.lock = threading.Lock()

    def __iter__(self):
        return self

    def next(self):
        with self.lock:
            return self.it.next()

def threadsafe_generator(f):
    """A decorator that takes a generator function and makes it thread-safe.
    """
    def g(*a, **kw):
        return threadsafe_iter(f(*a, **kw))
    return g

@threadsafe_generator
def idGenerator(start=0):
    '''generates relevant id numbers starting from 0 as default'''
    i = start
    while True:
        yield i
        i +=1;

IDGENERATOR = idGenerator()

class MyEnt(UserDict.IterableUserDict):
    """A dictionary representation of a Synapse entity.

    Annotations are preprocessed for those that are containers and returned the first item.

    """

    def __init__(self, syn, d, projectId=None, benefactorId=None):
        self._syn = syn

        UserDict.IterableUserDict.__init__(self, d)

        for key in self.data.keys():
            if type(self.data[key]) is list and len(self.data[key]) > 0:
                self.data[key] = self.data[key][0]

        self.data.pop('annotations')

        # If not supplied, get the project and benefactor of the entity
        self.data['projectId'] = projectId or self._getProjectId(self._syn, self.data['id'])
        self.data['benefactorId'] = benefactorId or self._getBenefactorId(self._syn, self.data['id'])
        self.data['_id'] = "%s.%s" % (self.data['id'], self.data['versionNumber'])
        self.data['synId'] = self.data['id']

    @staticmethod
    def _getProjectId(syn, synId):
        return filter(lambda x: x['type'] == 'org.sagebionetworks.repo.model.Project',
                      self._syn.restGET("/entity/%s/path" % synId)['path'])[0]

    @staticmethod
    def _getBenefactorId(syn, synId):
        return syn._getACL(synId)['id']

def processEnt(syn, fileVersion, projectId, toIgnore = IGNOREME_NODETYPES):
    """Convert a Synapse versioned Entity from REST call to a MyEnt dictionary.

    """

    logger.info('Getting entity (%r.%r)' % (fileVersion['id'], fileVersion['versionNumber']))

    ent = MyEnt(syn, syn.get(fileVersion['id'],
                             version=fileVersion['versionNumber'],
                             downloadFile=False),
                projectId)

    #Remove containers by ignoring layers, projects, and previews
    if ent['entityType'] in toIgnore:
        logger.info("Bad entity type (%s)" % (ent['entityType'], ))
        return {}

    # ent['projectId'] = projectId
    # ent['benefactorId'] = syn._getACL(ent['id'])['id']

    tmp = dict()
    tmp['%s.%s' % (fileVersion['id'], fileVersion['versionNumber'])] = ent
    return tmp

def getVersions(syn, synapseId, projectId, toIgnore):
    """Convert versions rest call to entity dictionary.

    """

    entityDict = {}
    fileVersions = syn._GET_paginated('/entity/%s/version' % (synapseId, ), offset=1)
    map(lambda x: entityDict.update(processEnt(syn, x, projectId, toIgnore)), fileVersions)
    return entityDict

def synFileIdWalker(walker):
    for x in walker:
        yield x['file.id']

def processEntities(projectId, toIgnore = IGNOREME_NODETYPES):
    '''Get and format all entities with from a Project.

    '''

    p = multiprocessing.dummy.Pool(8)

    logger.info('Getting and formatting all entities from %s' % projectId)

    q = syn.chunkedQuery('select id from file where projectId=="%s"' % projectId)
    walker = synFileIdWalker(q)

    entityDict = dict()

    p.map(lambda x: entityDict.update(getVersions(syn, x, projectId, toIgnore)), walker)

    return entityDict

def safeGetActivity(entity):
    """Retrieve provenance associated with a particular entity.

    Adds the Synapse ID of the entity getting activity for as `entityId`
    """

    k, ent = entity

    try:
        logger.debug('Getting Provenance for: %s' % (k, ))
        prov = syn.getProvenance(ent['synId'], version=ent['versionNumber'])
        prov['entityId'] = k
        return prov
    # This should be handled in syn.getProvenance
    except synapseclient.exceptions.SynapseHTTPError:
        return None

def cleanUpActivity(activity):
    logger.debug('Cleaning up activity: %s' % activity)
    activity['_id'] = activity.pop('id')
    activity['concreteType'] = 'org.sagebionetworks.repo.model.provenance.Activity'

    return activity

def cleanUpActivities(activities):
    """remove all activity-less entities

    """

    logger.debug('Removing entities without activities.')

    returnDict = {v['entityId']: cleanUpActivity(v) for v in activities if v}

    return returnDict

def buildEdgesfromActivities(nodes, activities):
    """Construct directed edges based on provenance.

    """

    logger.info('Constructing directed edges based on provenance')

    new_nodes = dict()
    edges = list()

    for k, entity in nodes.items():
        logger.debug('processing entity: %s' % k)

        if k not in activities:
            continue

        activity = activities[k]

        #Determine if we have already seen this activity
        if activity['_id'] not in new_nodes:
            logger.debug("%s not in new_nodes" % (activity['_id'], ))
            new_nodes[activity['_id']]  = activity

            #Add input relationships
            for used in activity['used']:
                edges = addNodesandEdges(used, nodes, activity, edges)
        else:
            activity = new_nodes[activity['_id']]

        #Add generated relationship (i.e. out edge)
        edges.append({'_id': IDGENERATOR.next(),
                      # 'synId': activity['synId'],
                      '_inV': entity['_id'],
                      '_outV': activity['_id'],
                      '_label':'generatedBy',
                      'createdBy': activity['createdBy'],
                      'createdOn': activity['createdOn'],
                      'modifiedBy':activity['modifiedBy'],
                      'modifiedOn':activity['modifiedOn']})

    nodes.update(new_nodes)

    return edges

def addNodesandEdges(used, nodes, activity, edges):
    #add missing vertices to nodes with edges
    if used['concreteType']=='org.sagebionetworks.repo.model.provenance.UsedEntity':
        targetId = '%s.%s' %(used['reference']['targetId'],
                             used['reference'].get('targetVersionNumber'))
        if targetId not in nodes:
            try:
                ent = syn.get(used['reference']['targetId'], version=used['reference'].get('targetVersionNumber'),
                              downloadFile=False)
            except Exception as e:
                logger.error("Could not get %s (%s)\n" % (targetId, e))
                return edges

            logger.debug(dict(used=used['reference']['targetId'], version=used['reference'].get('targetVersionNumber')))
            # ent['benefactorId'] = syn._getACL(ent['id'])['id']
            ent = processEntDict(MyEnt(syn, ent))
            tmp = ent.pop('annotations')

            nodes[targetId] = ent

    elif used['concreteType'] =='org.sagebionetworks.repo.model.provenance.UsedURL':
        targetId = used['url']
        if not targetId in nodes:
            nodes[targetId]= {'_id': IDGENERATOR.next(),
                              'name': used.get('name'),
                              'url': used['url'],
                              'concreteType': used['concreteType']}
    #Create the incoming edges
    edges.append({'_id': IDGENERATOR.next(),
                  # 'synId': activity['synId'],
                  '_inV': activity['_id'],
                  '_outV': nodes[targetId]['_id'],
                  '_label': 'executed' if used.get('wasExecuted', False) else 'used',
                  'wasExecuted': used.get('wasExecuted', False),
                  'createdBy': activity['createdBy'],
                  'createdOn': activity['createdOn'],
                  'modifiedBy':activity['modifiedBy'],
                  'modifiedOn':activity['modifiedOn']})

    return edges


if __name__ == '__main__':
    import os

    # logger = logging.getLogger()
    # logger.setLevel(logging.INFO)

    parser = argparse.ArgumentParser(description=
                'Creates a json file based on provenance for all Synapse projects')
    parser.add_argument('--p', type=int, default=4, help='Specify the pool size for the multiprocessing module')
    parser.add_argument('--j', metavar='json', help='Input name of json outfile')
    parser.add_argument('-l', action='store_true', default=False, help='Load data from json file to Neo4j database')
    args = parser.parse_args()

    p = mp.Pool(args.p)
    if args.j:
        json_file = args.j
    else:
        json_file = 'graphSynapse.json'
    projects = syn.chunkedQuery("select id from project")
    nodes = dict()

    for proj in projects:
        if proj in SKIP_LIST:
            logger.info("Skipping %s" % proj)
            continue
        logger.info('Getting entities from %s' %proj['project.id'])
        nodes.update( getEntities( projectId = str(proj['project.id']) ) )
    logger.info('Fetched %i entities' %len(nodes))

    activities = p.map(safeGetActivity, nodes.items())
    activities = cleanUpActivities(activities)
    if len(activities) > 0:
        logger.info('%i activities found i.e. %0.2g%% entities have provenance' %(len(activities),
                                                                            float(len(nodes))/len(activities)))
    else:
        logger.info('This project lacks accessible information on provenance')

    edges = buildEdgesfromActivities(nodes, activities)
    logger.info('I have %i nodes and %i edges' %(len(nodes), len(edges)))
    with open(json_file, 'w') as fp:
        json.dump(OrderedDict([('vertices', nodes.values()), ('edges', edges)]), fp, indent=4)

    if args.l:
        logger.info('Connecting to Neo4j and authenticating user credentials')
        authenticate(db_info['machine'], db_info['username'], db_info['password'])
        db_dir = db_info['machine'] + "/db/data"
        graph = Graph(db_dir)

        try:
            ndb.json2neo4j(str(json_file), graph)
        except:
            logger.error('Error involving loading data from json file to Neo4j database')
            raise
