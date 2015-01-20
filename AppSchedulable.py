'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

from Schedulable import Schedulable
from Resources import Resources
from RMContainerInfo import RMContainerInfo
from IOTask import IOTask


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
        self._startTime = scheduler.getCurrentTime()
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
        return RMContainerInfo(containerID, applicationID, node, task, self._scheduler.getCurrentTime())
    
    
    def hasContainerForNode(self, priority, node):
        requests = self._app.getResourceRequests(priority)
        if len(requests) == 0:
            return False
        
        for task in requests:
            if Resources.allFitIn(task.getResource(), node.getCapacity()):
                return True
            
        return False
    
    
    def taskFitsInNode(self, task, node):
        if not self._scheduler.consideringIO():
            return Resources.fitsIn(task.getResource(), node.getAvailableResource())
        else:
            if task.getExpectedNode() == None:
                return Resources.allFitIn(task.getResource(), node.getAvailableResource())
            else:
                if task.getExpectedNode() == node:
                    return Resources.localFitIn(task.getResource(), node.getAvailableResource())
                else:
                    return Resources.remoteFitIn(task.getResource(), node.getAvailableResource(), task.getExpectedNode().getAvailableResource())
    
    
    def assignContainerToTask(self, node, priority, task, reserved):
        container = None
        if reserved:
            container = node.getReservedContainer()
        else:
            container = self.createContainer(node, task)
            
        if self.taskFitsInNode(task, node):
            self._app.allocate(node, priority, container)
            
            if reserved:
                # unserve
                print("unserve")
            
            # inform the node
            node.allocateContainer(container)
            
            return task.getResource()
        else:
            return Resources.none()
            
    
    def assignContainerByPriority(self, node, reserved):
        if reserved:
            container = node.getReservedContainer()
            priority = container.getTask().getPriority()
            if len(self._app.getResourceRequests(priority)) == 0:
                # unreserve the previous reserved container
                return Resources.none()
        
        prioritiesToTry = []
        if reserved:
            prioritiesToTry.append(node.getReservedContainer().getTask().getPriority())
        else:
            prioritiesToTry = self._app.getPriorities()
        
        for priority in prioritiesToTry:
            if not self.hasContainerForNode(priority, node):
                continue
            
            tasks = self._app.getResourceRequests(priority)
            #first, assign container to local request
            localRequests = [task for task in tasks if task.getExpectedNode() == node]
            if len(localRequests) > 0:
                return self.assignContainerToTask(node, priority, localRequests[0], reserved)
            
            #second, assign contaienr to any request
            anyRequests = [task for task in tasks if task.getExpectedNode() == None]
            if len(anyRequests) > 0:
                return self.assignContainerToTask(node, priority, anyRequests[0], reserved)
            
            #Lastly, assign container to other request
            otherRequests = [task for task in tasks if task not in localRequests and task not in anyRequests]
            if len(otherRequests) > 0:
                return self.assignContainerToTask(node, priority, otherRequests[0], reserved)
            
        return Resources.none()
    
    
    def assignContainerOnNode(self, node):
        return self.assignContainerByPriority(node, False) 
            