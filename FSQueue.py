'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

import abc
import sys

from Schedulable import Schedulable

class FSQueue(Schedulable):
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractmethod
    def __init__(self, name, parent, scheduler):
        super(FSQueue, self).__init__()
        self._name = name
        self._parent = parent
        self._scheduler = scheduler
        self._policy = None
        self._maxApps = sys.maxint
        self._bestMulResFitness = -1.0
        self._bestResourceEntropy = -1.0
        
        
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
    
    
    def setMultiResFitness(self, best):
        self._bestMulResFitness = best
        
        
    def getMultiResFitness(self):
        return self._bestMulResFitness
    
    
    def setResEntropy(self, entropy):
        self._bestResourceEntropy = entropy
        
        
    def getResEntropy(self):
        return self._bestResourceEntropy
        
    
    @abc.abstractmethod
    def recomputeShares(self):
        return
    
    
    @abc.abstractmethod
    def getChildQueues(self):
        return
    
    
    @abc.abstractmethod
    def getAllAppSchedulables(self):
        return