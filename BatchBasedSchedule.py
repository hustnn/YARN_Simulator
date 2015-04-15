'''
Created on Apr 8, 2015

@author: hustnn
'''

import Configuration

from JobGenerator import JobGenerator
from Utility import Utility

import time
import itertools
from datetime import datetime
from random import randint
from Cluster import Cluster
from YARNScheduler import YARNScheduler
from WorkloadGenerator import WorkloadGenerator
from RMContainerInfo import RMContainerInfo

import copy
import multiprocessing
import time

def startService(scheduler, newLaunchQueue, newAppsQueue, completedQueue):
    # start node update thread
    while True:
        while(not completedQueue.empty()):
            print("completed container")
            containerID = completedQueue.get()
            container = scheduler._launchedContainerDict[containerID]
            scheduler.completeContainer(container)
        
        while(not newAppsQueue.empty()):
            newApps = newAppsQueue.get()
            for k, v in newApps.items():
                for job in v:
                    scheduler.addApplication(job, k)
        
        scheduler.update()
        #print("before node update")
        for node in scheduler._cluster.getAllNodes():
            scheduler.nodeUpdate(node)
            for container in scheduler._newLaunchContainerList:
                print("launch container")
                newLaunchQueue.put(container)
            scheduler._newLaunchContainerList = []
        #print("after node update")   
        #time.sleep(1)
        #print("process: " + str(obj))


def execSimulation(clusterSize, queueName, jobList):
    cluster = Cluster(clusterSize)
    #queueWorkloads = {"queue1": workloadSet}
    
    #print("fair")
    fairScheduler =  YARNScheduler(cluster, True, 1.0)
    fairScheduler.createQueue("queue1", "MULTIFAIR", True, "root")
    
    workloadGen = WorkloadGenerator(Configuration.SIMULATION_PATH, Configuration.WORKLOAD_PATH, {queueName: jobList}, cluster)
    workloadGen.genWorkloadByList(queueName, copy.deepcopy(jobList))
    
    newLaunchQueue = multiprocessing.Queue()
    newAppsQueue = multiprocessing.Queue()
    completedQueue = multiprocessing.Queue()
    
    updateProcess = multiprocessing.Process(target = startService, name = "updateProcess", args = (fairScheduler, newLaunchQueue, newAppsQueue, completedQueue))
    updateProcess.start()
        
    simulationStepCount = 0
    while True:
        if workloadGen.allJobsSubmitted() and len(fairScheduler.getAllApplications()) == 0:
            break
        
        currentTime = simulationStepCount * Configuration.SIMULATION_STEP
        workloadGen.submitJobs(currentTime, fairScheduler)
        addedApps = fairScheduler.activateWaitingJobs(currentTime)
        if len(addedApps) > 0:
            print("put new app")
            newAppsQueue.put(addedApps)
        
        while(not newLaunchQueue.empty()):
            print("new receive container")
            newContainer = newLaunchQueue.get()
            fairScheduler.launchAllocatedContainer(newContainer["containerID"], newContainer["node"], newContainer["task"], newContainer["appID"])
        
        fairScheduler.simulate(Configuration.SIMULATION_STEP, currentTime)

        for container in fairScheduler._completedContaienrList:
            #print(container.getContainerID())
            completedQueue.put(container.getContainerID())
        fairScheduler._completedContaienrList = []
        
        simulationStepCount += 1
        
    fairMakespan = simulationStepCount * Configuration.SIMULATION_STEP
    fairFinishedApp = fairScheduler.getFinishedAppsInfo()
    
    #print("perf")
    cluster = Cluster(clusterSize)
    perfScheduler = YARNScheduler(cluster, True, 0.0)
    perfScheduler.createQueue("queue1", "MULTIFAIR", True, "root")
    
    workloadGen = WorkloadGenerator(Configuration.SIMULATION_PATH, Configuration.WORKLOAD_PATH, {queueName: jobList}, cluster)
    workloadGen.genWorkloadByList(queueName, copy.deepcopy(jobList))
        
    simulationStepCount = 0
    while True:
        if workloadGen.allJobsSubmitted() and len(perfScheduler.getAllApplications()) == 0:
            break
        #print(workloadGen.allJobsSubmitted(), len(fairScheduler.getAllApplications()))
        currentTime = simulationStepCount * Configuration.SIMULATION_STEP
        workloadGen.submitJobs(currentTime, perfScheduler)
        perfScheduler.simulate(Configuration.SIMULATION_STEP, currentTime)
        simulationStepCount += 1
        
    perfMakespan = simulationStepCount * Configuration.SIMULATION_STEP
    perfFinishedApp = perfScheduler.getFinishedAppsInfo()
    
    count = 0
    reduction = 0.0
    for k in perfFinishedApp.keys():
        count += 1
        tPerf = perfFinishedApp[k]
        tFair = fairFinishedApp[k]
        if tPerf > tFair:
            red = float(tPerf - tFair) / tFair
            reduction += red
            
    return {"fairness": str(reduction / count), "perf": str(1 - min(float(perfMakespan) / fairMakespan, 1))}


def genJob(num):
    fileName = Configuration.WORKLOAD_PATH + "workloadSet"
    f = open(fileName, "r")
    lines = f.readlines()
    f.close()
    jobCount = 1
    jobList = []
    for l in lines:
        for i in range(num):
            items = l.split(",")
            #numOfTask = int(items[0])
            #taskExecTime = int(items[1])
            #submissionTime = int(items[2])
            memory = int(items[3])
            cpu = int(items[4])
            disk = int(items[5])
            network = int(items[6])
            #job = JobGenerator.genComputeIntensitveJob(str(jobCount), numOfTask, memory, cpu, disk, network, taskExecTime, submissionTime)
            jobList.append((memory, cpu, disk, network))
            
    return jobList


def genJobCateList(jobCate, num):
    l = []
    for i in range(len(jobCate)):
        for j in range(num):
            l.append(jobCate[i])
            
    return l
        
def swap(l, i, j):
    tmp = l[i]
    l[i] = l[j]
    l[j] = tmp


RES = [0]


def checkSwapValid(jobList, i, j):
    for k in range(i, j):
        if jobList[k] == jobList[j]:
            return False
    return True


def genCombination(jobList, i, n):
    if (i == n - 1):
        #print(jobList)
        RES[0] = RES[0] + 1
        #RES.append(list(jobList))
        #RES.append(genAverageEntropy(jobList, 4))
        return
    else:
        for k in range(i, n):
            if checkSwapValid(jobList, i, k):
                swap(jobList, k, i)
                genCombination(jobList, i + 1, n)
                swap(jobList, k, i)


def genAverageEntropy(l, batchSize):
    totalEntropy = 0
    c = len(l) / batchSize
    for i in range(c):
        s = l[i * batchSize: (i + 1) * batchSize]
        entropy = Utility.calEntropyOfVectorList(s, 4)
        totalEntropy += entropy
    aveEn = float(totalEntropy) / c
    return aveEn
    #print(aveEn)


def getNextPermu(A , n):
    j = n - 2
    while(A[j] >= A[j + 1] and j >= 0):
        j -= 1
        
    if (j < 0):
        return False
    
    i = n - 1
    while(A[j] >= A[i]):
        i -= 1
        
    swap(A, j, i)
    
    l = j + 1
    r = n - 1
    while(l < r):
        swap(A, l, r)
        l += 1
        r -= 1
        
    return True


def genLexiPermu(A, n):
    num = 0
    sorted(A)
    while(True):
        num += 1
        print(A)
        if not getNextPermu(A, n):
            break
    print(num)
    
    
def swapItemByWindow(A, swapNum, windowNum, windowSize):
    for i in range(swapNum):
        r1 = randint(0, windowNum - 1)
        r2 = randint(0, windowNum - 1)
        w1 = A[r1]
        w2 = A[r2]
        e1 = randint(0, windowSize - 1)
        e2 = randint(0, windowSize - 1)
        tmp = w1[e1]
        w1[e1] = w2[e2]
        w2[e2] = tmp
    return A


def genWindowBasedList(A, windowSize):
    w = []
    for i in range(len(A) / windowSize):
        w.append(A[i * windowSize : (i + 1) * windowSize])
        
    return w


def sortWindowBasedList(windowList):
    for i in range(len(windowList)):
        windowList[i] = sorted(windowList[i])
    
    
def genAverageEntropyByWindow(windowList):
    totalEntropy = 0
    for w in windowList:
        entropy = Utility.calEntropyOfVectorList(w, 4)
        #print("entropy" + str(entropy))
        totalEntropy += entropy
    aveEn = float(totalEntropy) / len(windowList)
    return aveEn


def genJobsAccordingCategoryList(categoryList):
    jobs = []
    fileName = Configuration.WORKLOAD_PATH + "workloadSet"
    f = open(fileName, "r")
    lines = f.readlines()
    f.close()
    jobCount = 0
    for i in categoryList:
        #print(categoryList)
        jobCount += 1
        line = lines[i - 1]
        items = line.split(",")
        numOfTask = int(items[0])
        taskExecTime = int(items[1])
        submissionTime = int(items[2])
        memory = int(items[3])
        cpu = int(items[4])
        disk = int(items[5])
        network = int(items[6])
        job = JobGenerator.genComputeIntensitveJob(str(jobCount), numOfTask, memory, cpu, disk, network, taskExecTime, submissionTime)
        jobs.append(job)
    return jobs


def genAveragePerfFairForWindowList(windowList, clusterSize):
    #print(windowList)
    avePerf = 0
    aveFair = 0
    count = 0
    for w in windowList:
        #print(w)
        jobs = genJobsAccordingCategoryList(w)
        print("exec simu start")
        res = execSimulation(clusterSize, "queue1", jobs)
        print("exec simu end")
        count += 1
        avePerf += float(res["perf"])
        aveFair += float(res["fairness"])
    return avePerf / count, aveFair / count


def calAverageValueOfWindowBasedList(workloadScale = 10, windowSize = 10, repeatNum = 5, swapNum = 10, clusterSize = 1):
    l = genJobCateList([1,2,3,4], workloadScale)
    windowNum = len(l) / windowSize
    w = genWindowBasedList(l, windowSize)
    swapNumList = [0]
    
    for i in range(1, swapNum):
        for j in range(repeatNum):
            swapNumList.append(i)
    
    entropyToPerf = {}
    entropyToFairness = {}
    result = []
    
    swapNumCount = 0
    for i in swapNumList:
        beforeSwap = list(w)
        afterSwap = swapItemByWindow(beforeSwap, i, windowNum, windowSize)
        sortWindowBasedList(afterSwap)
        e = genAverageEntropyByWindow(afterSwap)
        perf, fairness = genAveragePerfFairForWindowList(afterSwap, clusterSize)
        #print(e, perf, fairness)
        entropyToPerf.setdefault(float('%0.1f'%e), []).append(perf)
        entropyToFairness.setdefault(float('%0.1f'%e), []).append(fairness)
        swapNumCount += 1
        #print(swapNumCount)
    
    for k in entropyToPerf.keys():
        result.append({"entropy": k, 
                       "perf": sum(entropyToPerf[k]) / len(entropyToPerf[k]), 
                       "fairness": sum(entropyToFairness[k]) / len(entropyToFairness[k])})
        
    sortedResult = sorted(result, key=lambda k: k['entropy'])
    for i in sortedResult:
        print i["entropy"], i["perf"], i["fairness"]
        
        
def calAverageValueOfWindowBasedListForDiffClusterSize(workloadScale = 20, windowSize = 10, repeatNum = 3, swapInternal = 1, swapNum = 10, clusterSizeList = []):
    l = genJobCateList([1,2,3,4], workloadScale)
    windowNum = len(l) / windowSize
    w = genWindowBasedList(l, windowSize)
    #swapNumList = [0]
    swapNumList = []
    
    swapList = []
    for i in range(swapNum + 1):
        swapList.append( max(1, swapInternal * i))
    
    for i in swapList:
        for j in range(repeatNum):
            swapNumList.append(i)
    
    resultForDifferentSize = {}
    for size in clusterSizeList:
        resultForDifferentSize[size] = [{}, {}, []]
    
    for i in swapNumList:
        beforeSwap = list(w)
        afterSwap = swapItemByWindow(beforeSwap, i, windowNum, windowSize)
        sortWindowBasedList(afterSwap)
        e = genAverageEntropyByWindow(afterSwap)
        print("entropy:" + str(float('%.1f'%e)))

        for size in clusterSizeList:
            perf, fairness = genAveragePerfFairForWindowList(afterSwap, size)
            print("size: " + str(size))
            print(perf, fairness)
            resultForDifferentSize[size][0].setdefault(float('%.1f'%e), []).append(perf)
            resultForDifferentSize[size][1].setdefault(float('%.1f'%e), []).append(fairness)
            
    for size in resultForDifferentSize.keys():
        result = []
        for k in resultForDifferentSize[size][0].keys():
            result.append({"entropy": k, 
                       "perf": sum(resultForDifferentSize[size][0][k]) / len(resultForDifferentSize[size][0][k]), 
                       "fairness": sum(resultForDifferentSize[size][1][k]) / len(resultForDifferentSize[size][1][k])})
            resultForDifferentSize[size][2] = sorted(result, key=lambda k: k['entropy'])
    
    for k, v in resultForDifferentSize.items():
        print("cluster size: " + str(k))
        print "Entropy", "Perf", "Fairness"
        for i in v[2]:
            print i["entropy"], i["perf"], i["fairness"]


if __name__ == '__main__':
    #jobList = genJob(4)
    
    #genCombination(jobList, 0, len(jobList))
    #print(len(RES))
    
    #print(str(datetime.now()))
    #l = genJobCateList([1, 1, 2, 2, 3, 3], 1)
    #genLexiPermu(l, len(l))
    #print(str(datetime.now()))
    
    '''genCombination(l, 0, len(l))
    print(RES[0])
    print(str(datetime.now()))'''
    
    # Gen combination by swapping
    '''for clusterSize in [1, 2, 3, 4, 5]:
        print("cluster size: " + str(clusterSize))
        print "Entropy", "Perf", "Fairness"
        calAverageValueOfWindowBasedList(20, 10, 10, clusterSize)'''
    
    #calAverageValueOfWindowBasedListForDiffClusterSize(24, 24, 10, 10, [1,2,4,6,8,10])
    #calAverageValueOfWindowBasedListForDiffClusterSize(800, 800, 1, 60, 10, [100, 200, 400, 600, 800])
    calAverageValueOfWindowBasedListForDiffClusterSize(10, 10, 1, 1, 10, [10])
    
    #print(Utility.calEntropyOfVectorList([1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4]))
    
    #ratios = [{"window": 20, "cluster": 5}, {"window": 40, "cluster": 10}, {"window": 80, "cluster": 20}, {"window": 160, "cluster": 40}]
    '''ratios = [{"window": 160, "cluster": 40}]
    for i in ratios:
        print("window: " + str(i["window"]) + " cluster: " + str(i["cluster"]))
        print "Entropy", "Perf", "Fairness"
        calAverageValueOfWindowBasedList(i["window"], 10, 10, i["cluster"])'''
    
    #calAverageValueOfWindowBasedList(24, 24, 10, 10, 6)