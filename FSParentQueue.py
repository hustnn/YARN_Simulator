'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

from FSQueue import FSQueue
from Resources import Resources

class FSParentQueue(FSQueue):
    '''
    classdocs
    '''

    def __init__(self, name, parent):
        '''
        Constructor
        '''
        super(FSParentQueue, self).__init__(name, parent)
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
            
    