'''
Created on Jan 10, 2015

@author: niuzhaojie
'''

from FSSchedulerNode import FSSchedulerNode
from Resource import Resource
import Configuration

class Cluster(object):
    '''
    classdocs
    '''
            
    def __init__(self, numOfNodes):
        '''
        Constructor
        '''
        self._nodeList = []
        self._fileList = {}
        for i in range(numOfNodes):
            self._nodeList.append(FSSchedulerNode(i, Resource(Configuration.NODE_MEMORY, 
                                                              Configuration.NODE_CPU,
                                                              Configuration.NODE_DISK_BANDWIDTH,
                                                              Configuration.NODE_NETWORK_BANDWIDTH)))
            
    
    def uploadFile(self, file):
        self._fileList[file.getFileName()] = file
        # assign blocks to cluster in round-robin way
        i = 0
        for block in file.getBlockList():
            self._nodeList[i].uploadFileBlock(block)
            block.assignToNode(self._nodeList[i])
            i = (i + 1) % len(self._nodeList)
            
            
    def getFile(self, filename):
        return self._fileList.get(filename)
    
    
    def getAllNodes(self):
        return self._nodeList