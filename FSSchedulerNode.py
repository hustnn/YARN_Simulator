'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

class FSSchedulerNode(object):
    '''
    classdocs
    '''


    def __init__(self, nodeID, resourceCapacity):
        '''
        Constructor
        '''
        self._nodeID = nodeID
        self._resourceCapacity = resourceCapacity
        self._availableResource = resourceCapacity
        self._averageDiskBandwidth = resourceCapacity._disk
        self._averageNetworkBandwidth = resourceCapacity._network
        self._blockList = []
        
        
    def uploadFileBlock(self, block):
        self._blockList.append(block)
        