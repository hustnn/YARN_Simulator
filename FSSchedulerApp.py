'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

from Priority import Priority
from Resources import Resources
from SchedulableStatus import SchedulableStatus
from IOTask import IOTask

import time

class FSSchedulerApp(object):
    '''
    classdocs
    '''


    def __init__(self, job):
        '''
        Constructor
        '''
        self._requests = {}
        #self._applicationID = applicationID
        self._job = job
        self.updateResourceRequests(job.getTaskList())
        self._queue = None
        self._containerCount = 0
        self._liveContainers = []
        self._allAllocatedContainers = []
        self._currentConsumption = Resources.createResource(0, 0, 0, 0)
        
        
    def getApplicationID(self):
        return self._applicationID
    
        
    def getCurrentConsumption(self):
        return self._currentConsumption
    
    
    def getResourceRequests(self, priority):
        return self._requests.get(priority, [])
    
        
    def setAppSchedulable(self, appSchedulable):
        self._appSchedulable = appSchedulable
        
        
    def getQueue(self):
        return self._queue
    
    
    def getLiveContainers(self):
        return self._liveContainers
        
        
    def updateResourceRequests(self, taskList):
        for task in taskList:
            self._requests.setdefault(task.getPriority(), []).append(task)
            
            
    def assignToQueue(self, queue):
        self._queue = queue
            
            
    def getPriorities(self):
        # from high to low
        priorities = []
        for i in Priority:
            priorities.append(i)
            
        priorities.reverse()
        return priorities
    
    
    def getNewContainerID(self):
        self._containerCount += 1
        return str(self._job.getJobID() + "-" + str(self._containerCount))
    
    
    def updateResourceConsumptionOfTask(self, task):
        if type(task) is IOTask:
            if task.getExpectedNode() == None:
                task.getAllocatedNode().addDiskConsumingTask(task)
                task.getAllocatedNode().addNetworkConsumingTask(task)
            elif task.getExpectedNode() == task.getAllocatedNode():
                task.getAllocatedNode().addDiskConsumingTask(task)
            elif task.getExpectedNode() != task.getAllocatedNode():
                task.getAllocatedNode().addDiskConsumingTask(task)
                task.getAllocatedNode().addNetworkConsumingTask(task)
                task.getExpectedNode().addDiskConsumingTask(task)
                task.getExpectedNode().addNetworkConsumingTask(task)
                
                
    def clearResourceConsumptionOfTask(self, task):
        if type(task) is IOTask:
            if task.getExpectedNode() == None:
                task.getAllocatedNode().removeDiskConsumingTask(task)
                task.getAllocatedNode().removeNetworkConsumingTask(task)
            elif task.getExpectedNode() == task.getAllocatedNode():
                task.getAllocatedNode().removeDiskConsumingTask(task)
            elif task.getExpectedNode() != task.getAllocatedNode():
                task.getAllocatedNode().removeDiskConsumingTask(task)
                task.getAllocatedNode().removeNetworkConsumingTask(task)
                task.getExpectedNode().removeDiskConsumingTask(task)
                task.getExpectedNode().removeNetworkConsumingTask(task)
    
    
    def allocate(self, node, priority, container):
        # task --> running
        task = container.getTask()
        task.updateStatus(SchedulableStatus.RUNNING)
        self.updateResourceConsumptionOfTask(task)
        
        self._liveContainers.append(container)
        self._allAllocatedContainers.append(container)
        
        if priority not in self._requests and container.getTask() not in self._requests[priority]:
            raise Exception("task not in requests")
        self._requests[priority].remove(container.getTask())
        
        Resources.addTo(self._currentConsumption, container.getTask().getResource())
    
    
    def containerCompleted(self, container):
        # task --> finished
        task = container.getTask()
        task.updateStatus(SchedulableStatus.FINISHING)
        self.clearResourceConsumptionOfTask(task)
        
        container.setFinishTime(int(time.time()))
        self._liveContainers.remove(container)
        containerResource = container.getTask().getResource()
        Resources.subtractFrom(self._currentConsumption, containerResource)
        
        
    def clearRequests(self):
        self._requests.clear()
        
        