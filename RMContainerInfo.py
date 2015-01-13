'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

import time

class RMContainerInfo(object):
    '''
    classdocs
    '''


    def __init__(self, containerID, applicationID, node, task):
        '''
        Constructor
        '''
        self._containerID = containerID
        self._applicationID = applicationID
        self._node = node
        self._task = task
        self._startTime = int(time.time())
        self._finishTime = self._startTime
        
        
    def getTask(self):
        return self._task
        
        
    def setFinishTime(self, finishTime):
        self._finishTime = finishTime
        