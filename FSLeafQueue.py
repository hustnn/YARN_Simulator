'''
Created on Jan 12, 2015

@author: niuzhaojie
'''
from FSQueue import FSQueue
from Resources import Resources
from AppSchedulable import AppSchedulable

class FSLeafQueue(FSQueue):
    '''
    classdocs
    '''


    def __init__(self, name, parent, scheduler):
        '''
        Constructor
        '''
        super(FSLeafQueue, self).__init__(name, parent, scheduler)
        self._appScheds = []
        self._demand = Resources.createResource(0, 0, 0, 0)
        
        
    def addApp(self, app):
        appSchedulable = AppSchedulable(self._scheduler, app, self)
        app.setAppSchedulable(appSchedulable)
        self._appScheds.append(appSchedulable)
        
        
    def removeApp(self, app):
        appSchedulableToRemove = None
        for appSchedulable in self._appScheds:
            if appSchedulable.getApp() == app:
                appSchedulableToRemove = appSchedulable
        if appSchedulableToRemove != None:
            self._appScheds.remove(appSchedulableToRemove)
            
            
    def getAppSchedulables(self):
        return self._appScheds
    
    
    def getChildQueues(self):
        return []
            
            
    def recomputeShares(self):
        self._policy.computeShares(self._appScheds, self.getFairShare())
        
        
    def getDemand(self):
        return self._demand
    
    
    def getResourceUsage(self):
        usage = Resources.createResource(0, 0, 0, 0)
        for appSchedulable in self._appScheds:
            Resources.addTo(usage, appSchedulable.getResourceUsage())
            
        return usage
    
    
    def updateDemand(self):
        self._demand = Resources.createResource(0, 0, 0, 0)
        for appSchedulable in self._appScheds:
            appSchedulable.updateDemand()
            toAdd = appSchedulable.getDemand()
            Resources.addTo(self._demand, toAdd)
            
            
    def assignContainer(self, node):
        assigned = Resources.none()
        
        if node.getReservedContainer() != None:
            return assigned
        
        self._appScheds.sort(self._policy.getComparator())
        
        for sched in self._appScheds:
            assigned = sched.assignContainer(node)
            if not Resources.equals(assigned, Resources.none()):
                break
            
        return assigned