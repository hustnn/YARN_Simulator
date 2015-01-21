'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

from FSQueue import FSQueue
from Resources import Resources
from policies.PolicyParser import PolicyParser

import math

class FSParentQueue(FSQueue):
    '''
    classdocs
    '''

    def __init__(self, name, parent, scheduler):
        '''
        Constructor
        '''
        super(FSParentQueue, self).__init__(name, parent, scheduler)
        self._childQueues = []
        self._demand = Resources.createResource(0, 0, 0, 0)
        
    
    def addChildQueue(self, child):
        self._childQueues.append(child)
        
        
    def recomputeShares(self):
        self._policy.computeShares(self._childQueues, self.getFairShare())
        for childQueue in self._childQueues:
            childQueue.recomputeShares()
            
            
    def getDemand(self):
        return self._demand
    
    
    def getResourceUsage(self):
        usage = Resources.createResource(0, 0, 0, 0)
        for child in self._childQueues:
            Resources.addTo(usage, child.getResourceUsage())
        return usage
    
    
    def updateDemand(self):
        self._demand = Resources.createResource(0, 0, 0, 0)
        for childQueue in self._childQueues:
            childQueue.updateDemand()
            toAdd = childQueue.getDemand()
            Resources.addTo(self._demand, toAdd)    
        
    
    def assignContainer(self, node):
        assigned = Resources.none()
        
        if node.getReservedContainer() != None:
            return assigned
        
        # performance and fairness tradeoff
        selectivity = 1 - self._scheduler.getTradeoff()
        
        # first, sort by current policy
        self._childQueues.sort(self._policy.getComparator())
        # second, filtering according to selectivity
        end = min(len(self._childQueues), max(1, math.ceil(len(self._childQueues) * selectivity)))
        selectedChildQueues = self._childQueues[0 : end]
        # third, order the selected list according fitness
        multiResFitnessComparator = PolicyParser.getInstance("MRF", self._scheduler.getClusterCapacity()).getComparator()
        selectedChildQueues.sort(multiResFitnessComparator)
        
        for child in selectedChildQueues:
            assigned = child.assignContainer(node)
            if not Resources.equals(assigned, Resources.none()):
                break
            
        if Resources.equals(assigned, Resources.none()):
            for child in self._childQueues[end:]:
                assigned = child.assignContainer(node)
                if not Resources.equals(assigned, Resources.none()):
                    break
        
        
        # default implementation
        '''
        self._childQueues.sort(self._policy.getComparator())
        for child in self._childQueues:
            assigned = child.assignContainer(node)
            if not Resources.equals(assigned, Resources.none()):
                break'''
            
        return assigned
        
        
    def getChildQueues(self):
        return self._childQueues