'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

import abc

from Schedulable import Schedulable

class FSQueue(Schedulable):
    __metaclass__ = abc.ABCMeta
    
    
    def __init__(self, name, parent, scheduler):
        self._name = name
        self._parent = parent
        self._scheduler = scheduler
        self._policy = None
        
        
    def getName(self):
        return self._name
    
    
    def getPolicy(self):
        return self._policy
    
    
    @abc.abstractmethod
    def setPolicy(self, policy):
        return
    
    
    def getStartTime(self):
        return 0;
    
    
    @abc.abstractmethod
    def recomputeShares(self):
        return
    
    
    @abc.abstractmethod
    def getChildQueues(self):
        return