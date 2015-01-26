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
from SimilarityType import SimilarityType

import sys
import math

class YARNScheduler(object):
    '''
    classdocs
    '''


    def __init__(self, cluster, consideringIO = True, tradeoff = 1.0, similarityType = SimilarityType.PRODUCT):
        '''
        Constructor
        '''
        self._cluster = cluster
        self._clusterCapacity = Resources.createResource(0, 0, 0, 0)
        self.initClusterCapacity()
        self._consideringIO = consideringIO
        self._rootQueue = FSParentQueue("root", None, self)
        self._rootQueue.setPolicy(PolicyParser.getInstance("fair", self._clusterCapacity))
        self._queues = {"root": self._rootQueue}
        self._applications = []
        self._waitingJobList = {}
        self._currentTime = 0
        self._tradeoff = tradeoff
        self._similarityType = similarityType
        self._finishedApps = []
        
        
    def initClusterCapacity(self):
        for node in self._cluster.getAllNodes():
            Resources.addTo(self._clusterCapacity, node.getCapacity())
    

    def getCurrentTime(self):
        return self._currentTime
    
    
    def getTradeoff(self):
        return self._tradeoff
    
    
    def getClusterCapacity(self):
        return self._clusterCapacity
    
    
    def getFinishedApps(self):
        return self._finishedApps
        
        
    def createQueue(self, queueName, policy, isLeaf, parentQueueName, maxApps = sys.maxint):
        parentQueue = self._queues.get(parentQueueName)
        if isLeaf:
            queue = FSLeafQueue(queueName, parentQueue, self)
        else:
            queue = FSParentQueue(queueName, parentQueue, self)
            
        if parentQueue != None:
            parentQueue.addChildQueue(queue)
            
        queue.setMaxApps(maxApps)
        queue.setPolicy(PolicyParser.getInstance(policy, self._clusterCapacity))
        
        self._queues[queueName] = queue
        
        
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
        
        schedulerApp = FSSchedulerApp("App-" + job.getJobID(), job, self)
        
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
                
                self.calMultiResourceFitness(self._rootQueue, node)
                
                assignedResource = self._rootQueue.assignContainer(node)
                
                #if assignedResource.getMemory() > Resources.none().getMemory():
                #    assignedContainer = True
                if Resources.greaterAtLeastOne(assignedResource, Resources.none()):
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
        self._rootQueue.updateDemand()
        self._rootQueue.setFairShare(self._clusterCapacity)
        self._rootQueue.recomputeShares()
    
    
    def schedule(self, step):
        for app in self._applications:
            for liveContainer in app.getLiveContainers():
                liveContainer.getTask().schedule(step)
                
                
    def updateStatusAfterScheduling(self, currentTime):
        # update status of running containers
        finishedContainerList = []
        for app in self._applications:
            for liveContainer in app.getLiveContainers():
                task = liveContainer.getTask()
                if task.getWorkload() == 0:
                    finishedContainerList.append(liveContainer)
                    
        for container in finishedContainerList:
            self.completeContainer(container)
            
        # update status of running jobs
        finishedApps = []
        for app in self._applications:
            job = app.getJob()
            if job.allTasksFinished():
                job.updateStatus(SchedulableStatus.FINISHING)
                finishedApps.append(app)
            else:
                job.updateStatusOfPendingTasks()
                
        for app in finishedApps:
            self.removeApplication(app)
            app.getJob().setFinishTime(currentTime)
            self._finishedApps.append(app)
        
        
    def simulate(self, step, currentTime):
        self._currentTime = currentTime
        self.activateWaitingJobs(currentTime)
        
        self.update()
        
        for node in self._cluster.getAllNodes():
            #print("node id : " + str(node))
            self.nodeUpdate(node)
            #print(node.getAvailableResource())
            
        #check locality
        '''totalTask = 0
        localityTask = 0
        remoteTask = 0
        anyTask = 0
        for app in self._applications:
            for liveContainer in app.getLiveContainers():
                task = liveContainer.getTask()
                totalTask += 1
                if task.getExpectedNode() == None:
                    anyTask += 1
                else:
                    if task.getExpectedNode() == task.getAllocatedNode():
                        localityTask += 1
                    else:
                        remoteTask += 1
                        
        print(totalTask, localityTask, remoteTask, anyTask)'''
            
        for node in self._cluster.getAllNodes():
            node.calDiskBandwidth()
            node.calNetworkBandwidth()
                    
        self.schedule(step)
        self.updateStatusAfterScheduling(currentTime)
        
        
    def calMultiResourceFitness(self, queue, node):
        comparator = queue.getPolicy().getComparator()
        maxMulResFitness = -1.0
        selectivity = 1 - self._tradeoff
        
        if type(queue) is FSLeafQueue:
            apps = queue.getAppSchedulables()
            
            # calculate the fitness for all applications
            for app in apps:
                app.updateDemand()
                app.calMultiResFitness(node, self._similarityType)
                #print(app.getMultiResFitness(), str(node.getAvailableResource()))
            
            # first, sort by default policy of the current queue
            apps.sort(comparator) 
            # second, filtering
            end = int(min(len(apps), max(1, math.ceil(len(apps) * selectivity))))
            selectedApps = apps[0 : end]
            
            # get best fit app in the filtered list
            for app in selectedApps:
                mulResFitness = app.getMultiResFitness()
                if mulResFitness > maxMulResFitness:
                    maxMulResFitness = mulResFitness
                    
            queue.setMultiResFitness(maxMulResFitness)
        else:
            childQueues = queue.getChildQueues()
            
            # cal fitness for all child queues
            for child in childQueues:
                child.updateDemand()
                self.calMultiResourceFitness(child, node)
                #print(child.getMultiResFitness(), str(node.getAvailableResource()))
                
                
            # first, order by default policy of the current queue
            childQueues.sort(comparator)
            # second, filtering
            end = int(min(len(childQueues), max(1, math.ceil(len(childQueues) * selectivity))))
            selectedChildQueues = childQueues[0 : end]
            
            # get best fit child queue in the filtered list
            for child in selectedChildQueues:
                mulResFitness = child.getMultiResFitness()
                if mulResFitness > maxMulResFitness:
                    maxMulResFitness = mulResFitness
                    
            queue.setMultiResFitness(maxMulResFitness)
                