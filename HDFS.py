'''
Created on Jan 9, 2015

@author: niuzhaojie
'''

from FSSchedulerNode import FSSchedulerNode
from Resource import Resource
import Configuration

class HDFS(object):
    '''
    classdocs
    '''


    def __init__(self, numOfNodes):
        '''
        Constructor
        '''
        self._nodeList = []
        for i in range(numOfNodes):
            self._nodeList.append(FSSchedulerNode(i, Resource(Configuration.NODE_MEMORY, 
                                                              Configuration.NODE_CPU,
                                                              Configuration.NODE_DISK_BANDWIDTH,
                                                              Configuration.NODE_NETWORK_BANDWIDTH)))
        
        