'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

from FSParentQueue import FSParentQueue
from FSLeafQueue import FSLeafQueue
from policies.PolicyParser import PolicyParser
from FSSchedulerApp import FSSchedulerApp
from Resources import Resources
from SchedulableStatus import SchedulableStatus

import time

class YARNScheduler(object):
    '''
    classdocs
    '''


    def __init__(self, cluster):
        '''
        Constructor
        '''
        self._consideringIO = True
        self._rootQueue = FSParentQueue("root", None, self)
        self._rootQueue.setPolicy(PolicyParser.getInstance("fair"))
        self._queues = {"root": self._rootQueue}
        self._cluster = cluster
        self._applications = []
        self._waitingJobList = {}
        self._currentTime = 0


    def getCurrentTime(self):
        return self._currentTime
        
        
    def createQueue(self, queueName, maxApps, policy, isLeaf, parentQueueName):
        parentQueue = self._queues.get(parentQueueName)
        if isLeaf:
            queue = FSLeafQueue(queueName, parentQueue, self)
        else:
            queue = FSParentQueue(queueName, parentQueue, self)
            
        if parentQueue != None:
            parentQueue.addChildQueue(queue)
            
        queue.setMaxApps(maxApps)
        queue.setPolicy(PolicyParser.getInstance(policy))
        
        
    def consideringIO(self):
        return self._consideringIO
    
    
    def completeContainer(self, container):
        application = container.getApplication()
        node = container.getNode()
        
        application.containerCompleted(container)
        node.releaseContainer(container)
    
    
    def getAllApplications(self):
        return self._applications
    
    
    def addApplication(self, job, queueName):
        queue = self._queues.get(queueName)
        
        schedulerApp = FSSchedulerApp(job, self)
        
        queue.addApp(schedulerApp)
        schedulerApp.assignToQueue(queue)
        self._applications.append(schedulerApp)
        
        
    def removeApplication(self, app):
        self._applications.remove(app)
        
        for container in app.getLiveContainers():
            self.completeContainer(container)
            
        app.clearRequests()
        
        queue = self._queues.get(app.getQueue().getName())
        queue.removeApp(app)
        
        
    def nodeUpdate(self, node):
        # assign new containers
        # 1. check for reserved applications
        # 2. schedule if there are no reservations
        
        reservedAppSchedulable = node.getReservedAppSchedulable()
        if reservedAppSchedulable == None:
            # no reservation, schedule at queue which is farthest below fair share
            while node.getReservedContainer() == None:
                assignedContainer = False
                if node.getAvailableResource() == Resources.none() and len(self._applications) == 0:
                    break
                
                assignedResource = self._rootQueue.assignContainer(node)
                if assignedResource.getMemory() > Resources.none().getMemory():
                    assignedContainer = True
                    
                if not assignedContainer:
                    break
        
        
    def submitJob(self, job, queueName):
        self._waitingJobList.setdefault(queueName, []).append(job)
        
        
    def activateWaitingJobs(self, currentTime):
        for k, v in self._waitingJobList.items():
            for job in v:
                job.updateStatus(SchedulableStatus.RUNNING)
                job.setStartTime(currentTime)
                job.activeAllTasks()
                self.addApplication(job, k)
                
        self._waitingJobList.clear()
        
        
    def update(self):
        return
    
    
    def schedule(self, step):
        for app in self._applications:
            for liveContainer in app.getLiveContainers():
                liveContainer.getTask().schedule(step)
        
        
    def simulate(self, step, currentTime):
        self._currentTime = currentTime
        self.activateWaitingJobs(currentTime)
        
        for node in self._cluster.getAllNodes():
            self.nodeUpdate(node)
            
        for node in self._cluster.getAllNodes():
            node.calDiskBandwidth()
            node.calNetworkBandwidth()
        
        self.schedule(step)
        self.update()
        
        