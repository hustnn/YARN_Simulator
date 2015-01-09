'''
Created on Jan 9, 2015

@author: niuzhaojie
'''
from SchedulableStatus import SchedulableStatus

class Task(object):
    '''
    classdocs
    '''
    def __init__(self, taskID, priority, resourceRequest):
        '''
        Constructor
        '''
        self._taskID = taskID
        self._priority = priority
        self._resourceRequest = resourceRequest
        self._status = SchedulableStatus.WAITING
        self._childs = []
        self._parents = []
        
        
    def addChild(self, child):
        self._childs.append(child)
        child._parents.append(self)
        
        
    def updateStatus(self, status):
        self._status = status
        
        
    def getRunnable(self):
        return self._Status == SchedulableStatus.RUNNING
        
    
        
    