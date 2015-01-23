'''
Created on Jan 16, 2015

@author: niuzhaojie
'''

from ResourceType import ResourceType

class ResourceWeights(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
        self._weights = {}
        self._weights[ResourceType.MEMORY] = 0
        self._weights[ResourceType.CPU] = 0
        self._weights[ResourceType.DISK] = 0
        self._weights[ResourceType.NETWORK] = 0
        
        
    def getWeight(self, resourceType):
        return self._weights[resourceType]
    
    
    def setWeight(self, resourceType, weight):
        self._weights[resourceType] = weight
        
        
    def getWeightsDict(self):
        return self._weights