import synapseclient
import load2Neo4jDB as ndb
from collections import OrderedDict
import multiprocessing.dummy as mp
import threading
import argparse
import logging
import json
import sys

syn = synapseclient.login()

NODETYPES = {0:'dataset',1: 'layer',2: 'project',3: 'preview',4: 'folder',
             5: 'analysis',6: 'step', 7: 'code',8: 'link',9: 'phenotypedata',
             10:'genotypedata',11:'expressiondata',12:'robject',
             13:'summary',14:'genomicdata',15:'page',16:'file',17:'table',
             18:'community'} #used in getEntities

IGNOREME_NODETYPES = [1,2,3]

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

newIdGenerator = idGenerator()#29602)
counter2 = idGenerator()

def processEntDict(ent, newId=newIdGenerator):
    for key in ent.keys():
        ent[key] = ent[key][0] if (type(ent[key]) is list and len(ent[key])>0) else ent[key]

    ent['_type']='vertex'
    ent['_id'] = newId.next()
    ent['synId'] = ent.pop('id')
    synId = ent['synId']
    versionNumber = ent['versionNumber']

    return ent

def getEntities(projectId, newId = newIdGenerator, toIgnore = IGNOREME_NODETYPES):
    '''get and format all entities with the inputted projectId'''
    logging.info('Getting and formatting all entities from %s' %projectId)
    query = syn.chunkedQuery('select * from entity where projectId=="%s"' %projectId)
    entityDict = dict()
    for ent in query:
        try:
            #Remove containers by ignoring layers, projects, and previews
            if ent['entity.nodeType'] in toIgnore:
                continue

            for key in ent.keys():
                new_key = '.'.join(key.split('.')[1:])
                item = ent.pop(key)
                ent[new_key] = item

            ent = processEntDict(ent)

            entityDict['%s.%s' %(ent['synId'],ent['versionNumber'])] = ent
            logging.info('Getting entity (%i): %s.%s' %(ent['_id'], ent['synId'],
                                             ent['versionNumber']))
            # #retrieve previous versions
            # if int(versionNumber) > 1:
            #     for version in range(1,int(versionNumber)):
            #         ent = syn.restGET('/entity/%s/version/%s' %(ent['synId'],version))
            #         for key in ent.keys():
            #             #remove the "entity" portion of query
            #             new_key = '.'.join(key.split('.')[1:])
            #             item = ent.pop(key)
            #             ent[new_key] = item[0] if (type(item) is list and len(item)>0) else item
            #         ent['_type']='vertex'
            #         ent['_id'] = newId.next()
            #         ent['synId'] = synId
            #         ent['versionNumber'] = version
            #         entityDict['%s.%s' %(ent['synId'],version)] = ent
            #         logging.info('Getting previous version of entity (%i): %s.%i' %(ent['_id'],
            #                                      ent['synId'], version))

        except synapseclient.exceptions.SynapseHTTPError as e:
            sys.stderr.write('Skipping current entity (%s) due to %s' % (str(ent['synId']), str(e)) )
            continue
    return entityDict

def safeGetActivity(entity):
    '''retrieve activity/provenance associated with a particular entity'''
    k, ent = entity
    try:
        print 'Getting Provenance for: %s (%i)' % (k,counter2.next())
        prov = syn.getProvenance(ent['synId'], version=ent['versionNumber'])
        return (k, prov)
    except synapseclient.exceptions.SynapseHTTPError:
        return (k, None)

def cleanUpActivities(activities, newId = newIdGenerator):
    '''remove all activity-less entities'''
    logging.info('Removing all activity-less entities')
    returnDict = dict()
    for k,activity in activities:
        logging.info('Cleaning up activity: %s' % k)
        if activity is None:
            continue
        activity['synId'] = activity.pop('id')
        activity['concreteType']='activity'
        activity['_id'] = newId.next()
        activity['_type'] = 'vertex'
        returnDict[k] = activity
    return returnDict

def buildEdgesfromActivities(nodes, activities, newId = newIdGenerator):
    '''construct directed edges based on provenance'''
    logging.info('Constructing directed edges based on provenance')
    new_nodes = dict()
    edges = list()
    for k, entity in nodes.items():
        print 'processing entity:', k
        if k not in activities:
            continue
        activity = activities[k]
        #Determine if we have already seen this activity
        if activity['synId'] not in new_nodes:
            new_nodes[activity['synId']]  = activity
            #Add input relationships
            for used in activity['used']:
                edges = addNodesandEdges(used, nodes, activity, edges)
        else:
            activity = new_nodes[activity['synId']]
        #Add generated relationship (i.e. out edge)
        edges.append({'_id': newId.next(),
                      '_inV': entity['_id'],
                      '_outV': activity['_id'],
                      '_type':'edge', '_label':'generatedBy',
                      'createdBy': activity['createdBy'],
                      'createdOn': activity['createdOn'],
                      'modifiedBy':activity['modifiedBy'],
                      'modifiedOn':activity['modifiedOn']})
    nodes.update(new_nodes)
    return edges

def addNodesandEdges(used, nodes, activity, edges, newId = newIdGenerator):
    #add missing vertices to nodes with edges
    if used['concreteType']=='org.sagebionetworks.repo.model.provenance.UsedEntity':
        targetId = '%s.%s' %(used['reference']['targetId'],
                             used['reference'].get('targetVersionNumber'))
        if targetId not in nodes:
            try:
                ent = syn.get(used['reference']['targetId'], version=used['reference'].get('targetVersionNumber'),
                              downloadFile=False)
            except Exception as e:
                print e.message
                return edges

            print dict(used=used['reference']['targetId'], version=used['reference'].get('targetVersionNumber'))
            ent = processEntDict(dict(ent))
            tmp = ent.pop('annotations')

            nodes[targetId] = ent

    elif used['concreteType'] =='org.sagebionetworks.repo.model.provenance.UsedURL':
        targetId = used['url']
        if not targetId in nodes:
            nodes[targetId]= {'_id': newId.next(),
                              '_type': 'vertex',
                              'name': used.get('name'),
                              'url': used['url'],
                              'concreteType': used['concreteType']}
    #Create the incoming edges
    edges.append({'_id': newId.next(),
                  '_inV': activity['_id'],
                  '_type': 'edge',
                  '_outV': nodes[targetId]['_id'],
                  '_label': 'used',
                  'wasExecuted': used.get('wasExecuted', False),
                  'createdBy': activity['createdBy'],
                  'createdOn': activity['createdOn'],
                  'modifiedBy':activity['modifiedBy'],
                  'modifiedOn':activity['modifiedOn']})

    return edges


if __name__ == '__main__':
    import os
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

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
            print "Skipping"
            continue
        print 'Getting entities from %s' %proj['project.id']
        nodes.update( getEntities( projectId = str(proj['project.id']) ) )
    logging.info('Fetched %i entities' %len(nodes))

    activities = p.map(safeGetActivity, nodes.items())
    activities = cleanUpActivities(activities)
    if len(activities) > 0:
        print '%i activities found i.e. %0.2g%% entities have provenance' %(len(activities),
                                                                            float(len(nodes))/len(activities))
    else:
        print 'This project lacks accessible information on provenance'

    edges = buildEdgesfromActivities(nodes, activities)
    logging.info('I have  %i nodes and %i edges' %(len(nodes), len(edges)))
    with open(json_file, 'w') as fp:
        json.dump(OrderedDict([('vertices', nodes.values()), ('edges', edges)]), fp, indent=4)

    if args.l:
        logging.info('Connecting to Neo4j and authenticating user credentials')
        with open(os.path.join(os.path.expanduser('~'), "credentials.json")) as creds:
            db_info=json.load(creds)
        authenticate(db_info['machine'], db_info['username'], db_info['password'])
        db_dir = db_info['machine'] + "/db/data"
        graph = Graph(db_dir)

        try:
            ndb.json2neo4j(str(json_file), graph)
        except:
            logging.error('Error involving loading data from json file to Neo4j database')
            raise
