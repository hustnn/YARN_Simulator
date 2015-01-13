'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

from Schedulable import Schedulable
from Resources import Resources
from RMContainerInfo import RMContainerInfo

import time

class AppSchedulable(Schedulable):
    '''
    classdocs
    '''


    def __init__(self, scheduler, app, queue):
        '''
        Constructor
        '''
        self._scheduler = scheduler
        self._app = app
        self._queue = queue
        self._startTime = int(time.time())
        self._demand = Resources.createResource(0, 0, 0, 0)
        
        
    def getApp(self):
        return self._app
    
    
    def updateDemand(self):
        self._demand = Resources.createResource(0, 0, 0, 0)
        Resources.addTo(self._demand, self._app.getCurrentConsumption())
        
        for p in self._app.getPriorities():
            for task in self._app.getResourceRequests(p):
                resource = task.getResource()
                Resources.addTo(self._demand, resource)
                
                
    def getDemand(self):
        return self._demand
    
    
    def getStartTime(self):
        return self._startTime
    
    
    def getResourceUsage(self):
        return self._app.getCurrentConsumption()
    
    
    def createContainer(self, node, task):
        containerID = self._app.getNewContainerID()
        applicationID = self._app.getApplicationID()
        return RMContainerInfo(containerID, applicationID, node, task)
    
    
    def assignContainerToTask(self, node, priority, task, reserved):
        capacity = task.getResource()
        available = node.getAvailableResource()
        container = self.createContainer(node, task)
    
    
    def assignContainer(self, node, reserved):
        if reserved:
            container = node.getReservedContainer()
            priority = container.getTask().getPriority()
            if len(self._app.getResourceRequests(priority)) == 0:
                # unreserve the previous reserved container
                return Resources.none()
        
        prioritiesToTry = []
        
        
        for priority in prioritiesToTry:
            resourceRequests = self._app.getResourceRequests(priority)
            #find the best fit request
            bestRequest = resourceRequests[0]
            
    
    def assignContainerOnNode(self, node):
        return self.assignContainer(node, False) 
            