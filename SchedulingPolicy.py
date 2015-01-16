'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

import abc


class SchedulingPolicy(object):
    '''
    classdocs
    '''
    __metaclass__ = abc.ABCMeta 
    
        
    @abc.abstractmethod
    def computeShares(self, schedulables, totalResource):
        return
    
    
    @abc.abstractmethod
    def getComparator(self):
        return
        
    
    def initialize(self, clusterCapacity):
        self._clusterCapacity = clusterCapacity