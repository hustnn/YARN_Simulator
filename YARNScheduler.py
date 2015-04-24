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
from Utility import Utility
from RMContainerInfo import RMContainerInfo

import sys
import math
import time
from random import randint

class YARNScheduler(object):
    '''
    classdocs
    '''


    def __init__(self, cluster, consideringIO = True, tradeoff = 1.0, similarityType = SimilarityType.PRODUCT, schedulingMode = "default", 
                 randomFactor = 0, batchSize = 20, vectorQuantinationNum = 20, entropy = 0.0):
        '''
        Constructor
        '''
        self._cluster = cluster
        self._clusterCapacity = Resources.createResource(0, 0, 0, 0)
        self.initClusterCapacity()
        self._consideringIO = consideringIO
        # all queues are based on root, root use fair policy and can not be changed
        self._rootQueue = FSParentQueue("root", None, self)
        self._rootQueue.setPolicy(PolicyParser.getInstance("fair", self._clusterCapacity))
        self._queues = {"root": self._rootQueue}
        self._applications = []
        self._applicationsDict = {}
        self._waitingJobList = {}
        self._currentTime = 0
        # 0 <= tradeoff knob <= 1, 0 indicates complete performance optimization, 1 indicates complete fairness 
        self._tradeoff = tradeoff
        self._similarityType = similarityType
        self._finishedApps = []
        
        # scheduling model
        self._schedulingMode = schedulingMode
        self._randomFactor = randomFactor
        self._batchSize = batchSize
        self._vectorQuantinationNum = vectorQuantinationNum
        self._entropyThreshold = entropy
        self._entropyOfLastBatch = 0
        self._appsScheduledInCurBatch = []
        self._appsScheduledInLastBatch = []
        self._batchPolicy = "fair"
        self._batchPolicyCmp = PolicyParser.getInstance("MULTIFAIR", self._clusterCapacity).getComparator()
        self._feedbackWaitThreshold = cluster.getClusterSize()
        
        #cluster utilization
        self._memory = []
        self._cpu = []
        self._disk = []
        self._network = []
        
        #multi processing 
        self._newLaunchContainerList = []
        self._completedContaienrList = []
        self._launchedContainerDict = {}
        
    
    def startService(self, q):
        # start node update thread
        pass
        
        
    def getUtilization(self):
        return self._memory, self._cpu, self._disk, self._network
    
        
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
    
    
    def getAllNodes(self):
        return self._cluster.getAllNodes()
    
    
    def getFinishedAppsInfo(self):
        ret = {}
        for app in self._finishedApps:
            ret[app.getApplicationID()] = app.getJob().getFinishTime()
            
        return ret
        
        
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
        self._applicationsDict[schedulerApp.getApplicationID()] = schedulerApp
        
        
    def removeApplication(self, app):
        self._applications.remove(app)
        del self._applicationsDict[app.getApplicationID()]
        
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
                #if node.getAvailableResource() == Resources.none() and len(self._applications) == 0:
                #    break
                if not Resources.allAvailable(node.getAvailableResource()) or len(self._applications) == 0:
                    break
                
                self.calMultiResourceFitness(self._rootQueue, node)
                
                #default hadoop scheduling algorithm: call parent queue assign container method, and then leaf queue assign container method
                if self._schedulingMode == "default":
                    assignedResource = self._rootQueue.assignContainer(node)
                    #print(assignedResource)
                    if Resources.greaterAtLeastOne(assignedResource, Resources.none()):
                        assignedContainer = True
                        
                    if not assignedContainer:
                        break
                elif self._schedulingMode == "random":
                    # random seed generator
                    seed = randint(0, 99)
                    if self._randomFactor < seed:
                        # use default scheduling
                        assignedResource = self._rootQueue.assignContainer(node)
                        if Resources.greaterAtLeastOne(assignedResource, Resources.none()):
                            assignedContainer = True
                        
                        if not assignedContainer:
                            break
                    else:
                        # use random scheduling
                        applications = self._rootQueue.getAllAppSchedulables()
                        if len(applications) > 0:
                            index = randint(0, len(applications) - 1)
                            app = applications[index]
                            assignedResource = app.assignContainer(node)
                            if Resources.greaterAtLeastOne(assignedResource, Resources.none()):
                                assignedContainer = True
                        
                        if not assignedContainer:
                            break
                elif self._schedulingMode == "dynamic":
                    windowSize = 400
                    #print(len(self._appsScheduledInCurBatch))
                    if len(self._appsScheduledInCurBatch) == 0:
                        allApplications = self._rootQueue.getAllAppSchedulables()
                        applications = [app for app in allApplications if not Resources.equals(app.getCurrentResourceDemand(), Resources.none())]
                        self._appsScheduledInCurBatch = applications[0: min(windowSize, len(applications))]
                        #print(len(self._appsScheduledInCurBatch))
                        if len(self._appsScheduledInCurBatch) == 0:
                            break
                        resVectorList = []
                        for app in self._appsScheduledInCurBatch:
                            demand = app.getCurrentResourceDemand()
                            if not Resources.equals(demand, Resources.none()):
                                resVectorList.append(demand.getResourceVector())
                        
                        #print(resVectorList)
                        entropy = Utility.calEntropyOfResourceVectorList(resVectorList)
                        #print("entropy:" + str(entropy))
                        if entropy >= 2:
                            self._batchPolicy = "perf"
                            self._batchPolicyCmp = PolicyParser.getInstance("MRF", self._clusterCapacity).getComparator()
                        else:
                            self._batchPolicy = "fair"
                            self._batchPolicyCmp = PolicyParser.getInstance("MULTIFAIR", self._clusterCapacity).getComparator()
                    
                    if len(self._appsScheduledInCurBatch) > 0:
                        self._appsScheduledInCurBatch.sort(PolicyParser.getInstance("MULTIFAIR", self._clusterCapacity).getComparator())
                        self._appsScheduledInCurBatch.sort(self._batchPolicyCmp)
                        app = self._appsScheduledInCurBatch[0]
                        assignedResource = app.assignContainer(node)
                        if Resources.equals(app.getCurrentResourceDemand(), Resources.none()):
                            self._appsScheduledInCurBatch.remove(app)
                        if Resources.greaterAtLeastOne(assignedResource, Resources.none()):
                            assignedContainer = True
                        
                    if not assignedContainer:
                        break
                elif self._schedulingMode == "batch":
                    #batch scheduling, if batch scheduling list is empty, select k jobs (batch size) accroding to fairness
                    #decide the scheduling policy using rules
                    #if batch scheduling list is not empty, scheduling the job one by one according decided rule
                    if len(self._appsScheduledInCurBatch) == 0:
                        # feedback from last batch
                        waitingSign = False
                        for app in self._appsScheduledInLastBatch:
                            if app.getBlockCount() >= self._feedbackWaitThreshold:
                                waitingSign = True
                                break
                            
                        for app in self._appsScheduledInLastBatch:
                            app.setBlockCount(0)
                        self._appsScheduledInLastBatch = []
                        
                        # adjust entropy threshold according to the waiting sign and policy used in last batch
                        '''if waitingSign == False:
                            self._entropyThreshold -= 0.2
                        else:
                            self._entropyThreshold += 0.2
                            if self._batchPolicy == "perf":
                                self._entropyThreshold += 0.2
                            else:
                                self._entropyThreshold -= 0.2'''
                        
                        '''if self._entropyOfLastBatch < self._entropyThreshold:
                            self._batchSize -= 10
                        else:
                            self._batchSize += 10
                            
                        if self._batchSize < 1:
                            self._batchSize = 1'''
                        
                        #print(self._currentTime, waitingSign, self._batchPolicy, self._entropyThreshold, entropy)
                        
                        applications = self._rootQueue.getAllAppSchedulables()
                        fairPolicyCmp = PolicyParser.getInstance("MULTIFAIR", self._clusterCapacity).getComparator()
                        applications.sort(fairPolicyCmp)
                        self._appsScheduledInCurBatch = applications[0: min(len(applications), self._batchSize)]
                        # decide scheduling policy for this batch
                        entropy = Utility.calEntropyOfWorkload(self._appsScheduledInCurBatch, self._vectorQuantinationNum)
                        self._entropyOfLastBatch = entropy
                        
                        if entropy > self._entropyThreshold:
                            self._batchPolicy = "perf"
                            self._batchPolicyCmp = PolicyParser.getInstance("MRF", self._clusterCapacity).getComparator()
                        else:
                            self._batchPolicy = "fair"
                            self._batchPolicyCmp = PolicyParser.getInstance("MULTIFAIR", self._clusterCapacity).getComparator()
                        #print("entropy: " + str(entropy) + ", " + "policy: " + self._batchPolicy)
                        
                        #print(self._currentTime, waitingSign, self._batchPolicy, self._entropyThreshold, entropy)
                    
                    if len(self._appsScheduledInCurBatch) > 0:
                        # this batch ends until all jobs are scheduled for one time
                        self._appsScheduledInCurBatch.sort(PolicyParser.getInstance("MULTIFAIR", self._clusterCapacity).getComparator())
                        self._appsScheduledInCurBatch.sort(self._batchPolicyCmp)
                        app = self._appsScheduledInCurBatch[0]
                        assignedResource = app.assignContainer(node)
                        #print("app: " + app.getApp().getApplicationID() + ", assigned: " + str(assignedResource) + ", node: " + str(node) + ", fitness: " + str(app.getMultiResFitness()))
                        if Resources.greaterAtLeastOne(assignedResource, Resources.none()):
                            assignedContainer = True
                            self._appsScheduledInCurBatch.remove(app)
                            self._appsScheduledInLastBatch.append(app)
                        elif Resources.equals(assignedResource, Resources.none()):
                            # already finished, but the state has not been updated in time
                            self._appsScheduledInCurBatch.remove(app)
                            self._appsScheduledInLastBatch.append(app)
                        elif Resources.equals(assignedResource, Resources.notFit()):
                            # can not fit
                            app.setBlockCount(app.getBlockCount() + 1)
                    
                    if not assignedContainer:
                        break
                
        
    def submitJob(self, job, queueName):
        self._waitingJobList.setdefault(queueName, []).append(job)
        
        
    def activateWaitingJobs(self, currentTime):
        newJobs = {}
        for k, v in self._waitingJobList.items():
            for job in v:
                job.updateStatus(SchedulableStatus.RUNNING)
                job.setStartTime(currentTime)
                job.activeAllTasks()
                self.addApplication(job, k)
                newJobs.setdefault(k, []).append(job)
                
        self._waitingJobList.clear()
        return newJobs
        
        
    def update(self):
        self._rootQueue.updateDemand()
        self._rootQueue.setFairShare(self._clusterCapacity)
        self._rootQueue.recomputeShares()
    
    
    def schedule(self, step):
        for app in self._applications:
            app.scheduleLiveContaienrs(step)
                
                
    def updateStatusAfterScheduling(self, step, currentTime):
        # update status of running containers
        finishedContainerList = []
        for app in self._applications:
            for liveContainer in app.getLiveContainers()[:]:
                task = liveContainer.getTask()
                if task.getWorkload() == 0:
                    finishedContainerList.append(liveContainer)
                    
        for container in finishedContainerList:
            self.completeContainer(container)
            self._completedContaienrList.append(container)
            
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
            app.getJob().setFinishTime(currentTime + step)
            self._finishedApps.append(app)
            
            
    def oldSimulate(self, step, currentTime):
        self._currentTime = currentTime
        
        # compute fair share
        self.update()
        
        for node in self._cluster.getAllNodes():
            self.nodeUpdate(node)
            
        for node in self._cluster.getAllNodes():
            node.calDiskBandwidth()
            node.calNetworkBandwidth()

        totalMemory = 0
        totalCPU = 0
        totalDisk = 0
        totalNetwork = 0
        memory = 0
        cpu = 0
        disk = 0
        network = 0
        for node in self._cluster.getAllNodes():
            total = node.getCapacity()
            used = node.getUsedResource()
            totalMemory += total.getMemory()
            totalCPU += total.getCPU()
            totalDisk += total.getDisk()
            totalNetwork += total.getNetwork()
            memory += used.getMemory()
            cpu += used.getCPU()
            disk += used.getDisk()
            network += used.getNetwork()
            
        self._memory.append(float(memory) / totalMemory)
        self._cpu.append(float(cpu) / totalCPU)
        self._disk.append(float(disk) / totalDisk)
        self._network.append(float(network) / totalNetwork)
        
        self.schedule(step)
        self.updateStatusAfterScheduling(step, currentTime)
        
        
    def simulate(self, step, currentTime):
        self._currentTime = currentTime
        #self.activateWaitingJobs(currentTime)
        
        # compute fair share
        #self.update()
        
        #print("node update begin")
        
        #for node in self._cluster.getAllNodes():
        #    self.nodeUpdate(node)
            
        #print("node update end")
            
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
            
            
        # debug: print scheduled task info
        '''for node in self._cluster.getAllNodes():
            print(node.getNodeID())
            for container in node.getRunningContainers():
                print(container.getTask().getTaskID())'''
        
        totalMemory = 0
        totalCPU = 0
        totalDisk = 0
        totalNetwork = 0
        memory = 0
        cpu = 0
        disk = 0
        network = 0
        for node in self._cluster.getAllNodes():
            total = node.getCapacity()
            used = node.getUsedResource()
            totalMemory += total.getMemory()
            totalCPU += total.getCPU()
            totalDisk += total.getDisk()
            totalNetwork += total.getNetwork()
            memory += used.getMemory()
            cpu += used.getCPU()
            disk += used.getDisk()
            network += used.getNetwork()
            
        self._memory.append(float(memory) / totalMemory)
        self._cpu.append(float(cpu) / totalCPU)
        self._disk.append(float(disk) / totalDisk)
        self._network.append(float(network) / totalNetwork)
        
        #print("task schedule begin")
        self.schedule(step)
        #print("task schedule end")
        self.updateStatusAfterScheduling(step, currentTime)
        
        
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
                
                
    def launchAllocatedContainer(self, containerID, nodeID, taskID, appID):
        app = self._applicationsDict[appID]
        job = app.getJob()
        task = job._taskDict[taskID]
        node = self._cluster._nodeDict[nodeID]
        
        task.nodeAllocate(node)
        task.updateStatus(SchedulableStatus.RUNNING)
        
        container = RMContainerInfo(containerID, app, node, task, self.getCurrentTime())
        app._liveContainers.append(container)
        
        node.allocateContainer(container)