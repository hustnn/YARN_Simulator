'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

import abc
import sys

from Schedulable import Schedulable

class FSQueue(Schedulable):
    __metaclass__ = abc.ABCMeta
    
    
    def __init__(self, name, parent, scheduler):
        self._name = name
        self._parent = parent
        self._scheduler = scheduler
        self._policy = None
        self._maxApps = sys.maxint
        
        
    def setMaxApps(self, num):
        self._maxApps = num
        
        
    def getName(self):
        return self._name
    
    
    def getPolicy(self):
        return self._policy
    

    def setPolicy(self, policy):
        self._policy = policy
    
    
    def getStartTime(self):
        return 0;
    
    
    @abc.abstractmethod
    def recomputeShares(self):
        return
    
    
    @abc.abstractmethod
    def getChildQueues(self):
        return