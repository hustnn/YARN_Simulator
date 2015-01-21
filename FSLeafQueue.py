'''
Created on Jan 12, 2015

@author: niuzhaojie
'''
from FSQueue import FSQueue
from Resources import Resources
from AppSchedulable import AppSchedulable
from policies.PolicyParser import PolicyParser

import math

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
        
        # performance and fairness tradeoff
        selectivity = 1 - self._scheduler.getTradeoff()
        
        # first, sort by current policy
        self._appScheds.sort(self._policy.getComparator())
        # second, filtering
        end = min(len(self._appScheds), max(1, math.ceil(len(self._appScheds) * selectivity)))
        selectedApps = self._appScheds[0 : end]
        # thitd, sort selected list by fitness
        multiResFitnessComparator = PolicyParser.getInstance("MRF", self._scheduler.getClusterCapacity()).getComparator()
        selectedApps.sort(multiResFitnessComparator)
        
        for app in selectedApps:
            assigned = app.assignContainer(node)
            if not Resources.equals(assigned, Resources.none()):
                break
            
        if Resources.equals(assigned, Resources.none()):
            for app in self._appScheds[end:]:
                assigned = app.assignContainer(node)
                if not Resources.equals(assigned, Resources.none()):
                    break
        
        # default implementation
        '''
        self._appScheds.sort(self._policy.getComparator())
        for sched in self._appScheds:
            assigned = sched.assignContainer(node)
            if not Resources.equals(assigned, Resources.none()):
                break'''
            
        return assigned