'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

from Priority import Priority
from Resources import Resources
from SchedulableStatus import SchedulableStatus
from IOTask import IOTask


class FSSchedulerApp(object):
    '''
    classdocs
    '''


    def __init__(self, applicationID, job, scheduler):
        '''
        Constructor
        '''
        self._requests = {}
        self._applicationID = applicationID
        self._job = job
        self._scheduler = scheduler
        self.updateResourceRequests(job.getTaskList())
        self._queue = None
        self._containerCount = 0
        self._liveContainers = []
        self._allAllocatedContainers = []
        self._currentConsumption = Resources.createResource(0, 0, 0, 0)
        
        
    def getApplicationID(self):
        return self._applicationID
    
    
    def getJob(self):
        return self._job
    
        
    def getCurrentConsumption(self):
        return self._currentConsumption
    
    
    def getAnyResourceRequest(self):
        for requests in self._requests.values():
            if requests != None and len(requests) > 0:
                request = requests[0]
                if request.getStatus() == SchedulableStatus.RUNNABLE:
                    return requests[0].getResource()
            
        return "null"
    
    
    def getResourceRequests(self, priority):
        requests = self._requests.get(priority, [])
        runnableRequests = [request for request in requests if request.getStatus() == SchedulableStatus.RUNNABLE] 
        return runnableRequests
    
        
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
        priorities = [Priority.HIGH, Priority.NORMAL, Priority.LOW]
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
                
                
    def addCurrentConsumption(self, task):
        localResource = None
        if task.getExpectedNode() == task.getAllocatedNode():
            localResource = Resources.createResource(task.getResource().getMemory(), task.getResource().getCPU(), task.getResource().getDisk(), 0)
        else:
            localResource = task.getResource()
            
        remoteResource = None
        
        if task.getExpectedNode() != None and task.getExpectedNode() != task.getAllocatedNode():
            remoteResource = Resources.createResource(0, 0, localResource.getDisk(), localResource.getNetwork())
            
        Resources.addTo(self._currentConsumption, localResource)
        if remoteResource != None:
            Resources.addTo(self._currentConsumption, remoteResource)
            
            
    def subtractCurrentConsumption(self, task):
        localResource = None
        if task.getExpectedNode() == task.getAllocatedNode():
            localResource = Resources.createResource(task.getResource().getMemory(), task.getResource().getCPU(), task.getResource().getDisk(), 0)
        else:
            localResource = task.getResource()
            
        remoteResource = None
        
        if task.getExpectedNode() != None and task.getExpectedNode() != task.getAllocatedNode():
            remoteResource = Resources.createResource(0, 0, localResource.getDisk(), localResource.getNetwork())
            
        Resources.subtractFrom(self._currentConsumption, localResource)
        if remoteResource != None:
            Resources.subtractFrom(self._currentConsumption, remoteResource)
    
    
    def allocate(self, priority, container):
        # task --> running
        task = container.getTask()
        task.nodeAllocate(container.getNode())
        task.updateStatus(SchedulableStatus.RUNNING)
        self.updateResourceConsumptionOfTask(task)
        
        self._liveContainers.append(container)
        self._allAllocatedContainers.append(container)
        
        if priority not in self._requests and container.getTask() not in self._requests[priority]:
            raise Exception("task not in requests")
        self._requests[priority].remove(container.getTask())
        
        # currently, we only need use memory and cpu, so we just ignore the disk and network.
        # Otherwise, we need calculate the disk and network by considering locality
        #Resources.addTo(self._currentConsumption, container.getTask().getResource())
        self.addCurrentConsumption(container.getTask())
    
    
    def containerCompleted(self, container):
        # task --> finished
        task = container.getTask()
        task.updateStatus(SchedulableStatus.FINISHING)
        self.clearResourceConsumptionOfTask(task)
        
        container.setFinishTime(self._scheduler.getCurrentTime())
        self._liveContainers.remove(container) 
        #Resources.subtractFrom(self._currentConsumption, container.getTask().getResource())
        self.subtractCurrentConsumption(container.getTask())
        
        
    def clearRequests(self):
        self._requests.clear()
        
        