'''
Created on Jan 9, 2015

@author: niuzhaojie
'''
from SchedulableStatus import SchedulableStatus

class Task(object):
    '''
    classdocs
    '''
    def __init__(self, taskID, priority, resource):
        '''
        Constructor
        '''
        self._taskID = taskID
        self._priority = priority
        self._resource = resource
        self._status = SchedulableStatus.WAITING
        self._childs = []
        self._parents = []
        
        
    def addChild(self, child):
        self._childs.append(child)
        child._parents.append(self)
        
        
    def updateStatus(self, status):
        self._status = status
        
        
    def getResource(self):
        return self._resource
    
    
    def getPriority(self):
        return self._priority
        
    
        
    
        
    