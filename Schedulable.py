'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

import abc
from Resources import Resources

class Schedulable(object):
    __metaclass__ = abc.ABCMeta
    
    def __init__(self):
        self._fairShare = Resources.createResource(0, 0, 0, 0)
        self._weight = 1.0

        
    @abc.abstractmethod
    def getName(self):
        return
    
    @abc.abstractmethod
    def getDemand(self):
        return
    
    
    @abc.abstractmethod
    def getResourceUsage(self):
        return
    
    
    def setWeight(self, weight):
        self._weight = weight
        
    
    def getWeight(self):
        return self._weight
    
    
    @abc.abstractmethod
    def getStartTime(self):
        return
    
    
    @abc.abstractmethod
    def getPriority(self):
        return
    
    
    @abc.abstractmethod
    def updateDemand(self):
        return
    
    
    @abc.abstractmethod
    def assignContainer(self, node):
        return
    
    
    @abc.abstractmethod
    def setFairShare(self, resource):
        self._fairShare = resource
        
        
    @abc.abstractmethod
    def getFairShare(self):
        return self._fairShare
    
    
        
        