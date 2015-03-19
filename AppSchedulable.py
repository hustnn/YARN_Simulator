'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

from Schedulable import Schedulable
from Resources import Resources
from RMContainerInfo import RMContainerInfo
from SchedulableStatus import SchedulableStatus
from SimilarityType import SimilarityType
import Configuration


class AppSchedulable(Schedulable):
    '''
    classdocs
    '''


    def __init__(self, scheduler, app, queue):
        '''
        Constructor
        '''
        super(AppSchedulable, self).__init__()
        self._scheduler = scheduler
        self._app = app
        self._queue = queue
        self._startTime = scheduler.getCurrentTime()
        self._demand = Resources.createResource(0, 0, 0, 0)
        self._multipleResourceFitness = -1.0
        self._blockCount = 0
        
        
    def getBlockCount(self):
        return self._blockCount
    
    
    def setBlockCount(self, blockCount):
        self._blockCount = blockCount
        
        
    def getApp(self):
        return self._app
    
    
    def getName(self):
        return self._app.getApplicationID()
    
    
    def updateDemand(self):
        self._demand = Resources.createResource(0, 0, 0, 0)
        Resources.addTo(self._demand, self._app.getCurrentConsumption())
        
        for p in self._app.getPriorities():
            for task in self._app.getResourceRequests(p):
                if task.getStatus() == SchedulableStatus.RUNNABLE:
                    resource = task.getResource()
                    Resources.addTo(self._demand, resource)
                
                
    def getDemand(self):
        return self._demand
    
    
    def getStartTime(self):
        return self._startTime
    
    
    def getResourceUsage(self):
        return self._app.getCurrentConsumption()
    
    
    def getAnyResourceRequest(self):
        return self._app.getAnyResourceRequest()
    
    
    def createContainer(self, node, task):
        containerID = self._app.getNewContainerID()
        return RMContainerInfo(containerID, self._app, node, task, self._scheduler.getCurrentTime())
    
    
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
                #print("any" + ", request: " + str(task.getResource()) + ", node: " + str(node) + "," + str(node.getAvailableResource()))
                return Resources.allFitIn(task.getResource(), node.getAvailableResource())
            else:
                if task.getExpectedNode() == node:
                    #print("local" + ", request: " + str(task.getResource()) + ", node: " + str(node) + "," + str(node.getAvailableResource()))
                    return Resources.localFitIn(task.getResource(), node.getAvailableResource())
                else:
                    #print("remote" + ", request: " + str(task.getResource()) + ", node: " + str(node) + "," + str(node.getAvailableResource()) + ", expected: " + str(task.getExpectedNode()) + "," + str(task.getExpectedNode().getAvailableResource()))
                    return Resources.remoteFitIn(task.getResource(), node.getAvailableResource(), task.getExpectedNode().getAvailableResource())
    
    
    def assignContainerToTask(self, node, priority, task, reserved):
        container = None
        if reserved:
            container = node.getReservedContainer()
        else:
            container = self.createContainer(node, task)
        
        #debug
        '''expectedNode = "none"
        if task.getExpectedNode() != None:
            expectedNode = task.getExpectedNode().getNodeID()
        print("expected node: " + expectedNode)'''
            
        if self.taskFitsInNode(task, node):
            self._app.allocate(priority, container)
            
            if reserved:
                # unserve
                print("unserve")
            
            # inform the node
            node.allocateContainer(container)
            
            return task.getResource()
        else:
            #print(Resources.notFit())
            return Resources.notFit()
            
    
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
            
            #first, assign contaienr to any request
            anyRequests = [task for task in tasks if task.getExpectedNode() == None]
            if len(anyRequests) > 0:
                return self.assignContainerToTask(node, priority, anyRequests[0], reserved)
            
            #second, assign container to local request
            localRequests = [task for task in tasks if task.getExpectedNode() == node]
            if len(localRequests) > 0:
                return self.assignContainerToTask(node, priority, localRequests[0], reserved)
            
            #Lastly, assign container to other request
            otherRequests = [task for task in tasks if task not in localRequests and task not in anyRequests]
            if len(otherRequests) > 0:
                return self.assignContainerToTask(node, priority, otherRequests[0], reserved)
        
        return Resources.none()
    
    
    def assignContainer(self, node):
        return self.assignContainerByPriority(node, False) 
    
    
    def getMultiResFitness(self):
        return self._multipleResourceFitness
            
            
    def calMultiResFitness(self, node, similarityType = SimilarityType.PRODUCT):
        fitness = -1.0
        
        priorities = self._app.getPriorities()
        for priority in priorities:
            if not self.hasContainerForNode(priority, node):
                continue
            
            tasks = self._app.getResourceRequests(priority)
            
            #first, assign contaienr to any request
            anyRequests = [task for task in tasks if task.getExpectedNode() == None]
            if len(anyRequests) > 0:
                task = anyRequests[0]
                if self.taskFitsInNode(task, node):
                    if similarityType == SimilarityType.PRODUCT:
                        fitness = Resources.normalizedDotProduct(task.getResource(), node.getAvailableResource(), node.getCapacity(), self._scheduler.consideringIO())
                    elif similarityType == SimilarityType.COSINE:
                        fitness = Resources.cosineSimilarity(task.getResource(), node.getAvailableResource(), self._scheduler.consideringIO())
                    else:
                        fitness = 0.0
                else:
                    fitness = 0.0
                break
            
            #second, assign container to local request
            localRequests = [task for task in tasks if task.getExpectedNode() == node]
            if len(localRequests) > 0:
                task = localRequests[0]
                if self.taskFitsInNode(task, node):
                    localResource = Resources.createResource(task.getResource().getMemory(), task.getResource().getCPU(), 
                                                            task.getResource().getDisk(), 0)
                    #localResource = task.getResource()
                    if similarityType == SimilarityType.PRODUCT:
                        fitness = Resources.normalizedDotProduct(localResource, node.getAvailableResource(), node.getCapacity(), self._scheduler.consideringIO())
                    elif similarityType == SimilarityType.COSINE:
                        fitness = Resources.cosineSimilarity(localResource, node.getAvailableResource(), self._scheduler.consideringIO())
                    else:
                        fitness = 0.0
                else:
                    fitness = 0.0
                break
            
            #Lastly, assign container to other request
            otherRequests = [task for task in tasks if task not in localRequests and task not in anyRequests]
            if len(otherRequests) > 0:
                task = otherRequests[0]
                if self.taskFitsInNode(task, node):
                    localResource = Resources.createResource(task.getResource().getMemory(), task.getResource().getCPU(), 
                                                            task.getResource().getDisk(), 0)
                    if similarityType == SimilarityType.PRODUCT:
                        fitness = Resources.normalizedDotProduct(localResource, node.getAvailableResource(), node.getCapacity(), self._scheduler.consideringIO())
                    elif similarityType == SimilarityType.COSINE:
                        fitness = Resources.cosineSimilarity(localResource, node.getAvailableResource(), self._scheduler.consideringIO())
                    else:
                        fitness = 0.0
                    fitness = fitness * (1 - Configuration.REMOTE_PENALTY)
                else:
                    fitness = 0.0
                break
            
        self._multipleResourceFitness = fitness
        
        
    def getCurrentResourceDemand(self):
        prioritiesToTry = self._app.getPriorities()
        for priority in prioritiesToTry:
            tasks = self._app.getResourceRequests(priority)
            if (len(tasks) > 0):
                return tasks[0].getResource()
        
        return Resources.none()