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
            
    def __init__(self, numOfNodes, load = 0.0):
        '''
        Constructor
        '''
        self._nodeList = []
        self._nodeDict = {}
        self._fileList = {}
        for i in range(numOfNodes):
            node = FSSchedulerNode(i, Resource(Configuration.NODE_MEMORY * (1 - load), 
                                               Configuration.NODE_CPU * (1 - load),
                                               Configuration.NODE_DISK_BANDWIDTH * (1 - load),
                                               Configuration.NODE_NETWORK_BANDWIDTH * (1 - load)))
            self._nodeList.append(node)
            self._nodeDict[node.getNodeID()] = node
            
    
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
    
    
    def getClusterSize(self):
        return len(self._nodeList)