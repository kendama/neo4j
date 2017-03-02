import threading
import argparse
import logging
import json
import sys
from collections import OrderedDict
import multiprocessing
import UserDict
import uuid

import synapseclient
import synapseutils

import GraphToNeo4j

syn = synapseclient.login(silent=True)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

IGNOREME_NODETYPES = ['org.sagebionetworks.repo.model.Project',
                      'org.sagebionetworks.repo.model.Preview']

SKIP_LIST = ['syn582072', 'syn3218329', 'syn2044761', 'syn2351328', 'syn1450028']

class MyEntity(UserDict.IterableUserDict):
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
        self.data['projectId'] = projectId or self._getProjectId(self.data['id'])
        self.data['benefactorId'] = benefactorId or self._getBenefactorId(self.data['id'])
        self.data['_id'] = "%s.%s" % (self.data['id'], self.data['versionNumber'])
        self.data['synId'] = self.data.pop('id')

    def _getProjectId(self, synId):
        return filter(lambda x: x['type'] == 'org.sagebionetworks.repo.model.Project',
                      self._syn.restGET("/entity/%s/path" % synId)['path'])[0]['id']

    def _getBenefactorId(self, synId):
        return self._syn._getACL(synId)['id']

def processEnt(syn, fileVersion, projectId=None, benefactorId=None, toIgnore = IGNOREME_NODETYPES):
    """Convert a Synapse versioned Entity from REST call to a MyEntity dictionary.

    """

    logger.info('Getting entity (%r.%r)' % (fileVersion['id'], fileVersion['versionNumber']))
    ent = syn.get(fileVersion['id'],
                  version=fileVersion['versionNumber'],
                  downloadFile=False)

    #Remove containers by ignoring layers, projects, and previews
    if ent['entityType'] in toIgnore:
        logger.info("Bad entity type (%s)" % (ent['entityType'], ))
        return {}

    ent = MyEntity(syn, ent, projectId=projectId, benefactorId=benefactorId)
    k = '%s.%s' % (fileVersion['id'], fileVersion['versionNumber'])

    return {k: ent}

def getVersions(syn, synapseId, *args, **kwargs): #projectId=None, benefactorId=None, toIgnore=IGNOREME_NODETYPES):
    """Convert versions rest call to entity dictionary.

    """

    entityDict = {}
    fileVersions = syn._GET_paginated('/entity/%s/version' % (synapseId, ), offset=1)
    map(lambda x: entityDict.update(processEnt(syn, x, *args, **kwargs)), fileVersions)
    return entityDict

def processEntities(projectId, pool=multiprocessing.dummy.Pool(1), toIgnore = IGNOREME_NODETYPES):
    '''Get and format all entities with from a Project.

    '''

    logger.info('Getting and formatting all entities from %s' % projectId)

    q = syn.chunkedQuery('select id,benefactorId from file where projectId=="%s"' % projectId)

    entityDicts = pool.map(lambda x: getVersions(syn, synapseId=x['file.id'],
                                              projectId=projectId,
                                              benefactorId=x['file.benefactorId'],
                                              toIgnore=toIgnore), q)

    entityDict = reduce(lambda a, b: dict(a, **b), entityDicts)

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

def cleanUpActivities(activities):
    """remove all activity-less entities

    """

    returnDict = {}

    for activity in activities:
        logger.debug('Cleaning up activity: %s' % activity)

        if activity:
            activity['_id'] = activity.pop('id')
            activity['concreteType'] = 'org.sagebionetworks.repo.model.provenance.Activity'
            returnDict[activity['entityId']] = activity

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
        edges.append({'_inV': entity['_id'],
                      '_outV': activity['_id'],
                      '_label':'generatedBy'})

    nodes.update(new_nodes)

    return edges

def addNodesandEdges(used, nodes, activity, edges):
    #add missing vertices to nodes with edges
    if used['concreteType']=='org.sagebionetworks.repo.model.provenance.UsedEntity':
        targetId = '%s.%s' %(used['reference']['targetId'],
                             used['reference'].get('targetVersionNumber'))
        if targetId not in nodes:
            try:
                ent = syn.get(used['reference']['targetId'],
                              version=used['reference'].get('targetVersionNumber'),
                              downloadFile=False)
                ent = MyEntity(syn, ent)
            except Exception as e:
                logger.error("Could not get %s (%s)\n" % (targetId, e))
                return edges

            logger.debug(dict(used=used['reference']['targetId'], version=used['reference'].get('targetVersionNumber')))
            # ent['benefactorId'] = syn._getACL(ent['id'])['id']

            nodes[targetId] = ent

    elif used['concreteType'] =='org.sagebionetworks.repo.model.provenance.UsedURL':
        targetId = str(uuid.uuid3(uuid.NAMESPACE_URL, str(used['url'])))

        if not targetId in nodes:
            nodes[targetId]= {'_id': targetId,
                              'name': used.get('name'),
                              'url': used['url'],
                              'concreteType': used['concreteType']}
    #Create the incoming edges
    edges.append({'_inV': activity['_id'],
                  '_outV': nodes[targetId]['_id'],
                  '_label': 'executed' if used.get('wasExecuted', False) else 'used'})

    return edges
