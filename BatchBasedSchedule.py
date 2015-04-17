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
import math

def startNodaUpdateService(scheduler, newLaunchQueue, newAppsQueue, completedQueue, e):
    # start node update thread
    while True and not e.is_set():
        while(not completedQueue.empty()):
            #print("completed container")
            containerID = completedQueue.get()
            container = scheduler._launchedContainerDict[containerID]
            scheduler.completeContainer(container)
        
        while(not newAppsQueue.empty()):
            #print("new apps submitted")
            newApps = newAppsQueue.get()
            for k, v in newApps.items():
                for job in v:
                    scheduler.addApplication(job, k)
        
        scheduler.update()
        for node in scheduler._cluster.getAllNodes():
            scheduler.nodeUpdate(node)
            for container in scheduler._newLaunchContainerList:
                #print("launch container")
                newLaunchQueue.put(container)
            scheduler._newLaunchContainerList = []
        

def oldScheduling(clusterSize, queueName, jobList, tradeoff):        
    cluster = Cluster(clusterSize)

    scheduler =  YARNScheduler(cluster, True, tradeoff)
    scheduler.createQueue("queue1", "MULTIFAIR", True, "root")
    
    workloadGen = WorkloadGenerator(Configuration.SIMULATION_PATH, Configuration.WORKLOAD_PATH, {queueName: jobList}, cluster)
    workloadGen.genWorkloadByList(queueName, copy.deepcopy(jobList))
        
    simulationStepCount = 0
    while True:
        if workloadGen.allJobsSubmitted() and len(scheduler.getAllApplications()) == 0:
            break
        
        currentTime = simulationStepCount * Configuration.SIMULATION_STEP
        workloadGen.submitJobs(currentTime, scheduler)
        scheduler.activateWaitingJobs(currentTime)
        
        scheduler.oldSimulate(Configuration.SIMULATION_STEP, currentTime)
        
        simulationStepCount += 1
        
    makespan = simulationStepCount * Configuration.SIMULATION_STEP
    finishedApp = scheduler.getFinishedAppsInfo()
    return makespan, finishedApp

        
def scheduling(clusterSize, queueName, jobList, tradeoff):
    cluster = Cluster(clusterSize)
    #queueWorkloads = {"queue1": workloadSet}
    
    #print("fair")
    scheduler =  YARNScheduler(cluster, True, tradeoff)
    scheduler.createQueue("queue1", "MULTIFAIR", True, "root")
    
    workloadGen = WorkloadGenerator(Configuration.SIMULATION_PATH, Configuration.WORKLOAD_PATH, {queueName: jobList}, cluster)
    workloadGen.genWorkloadByList(queueName, copy.deepcopy(jobList))
    
    newLaunchQueue = multiprocessing.Queue()
    newAppsQueue = multiprocessing.Queue()
    completedQueue = multiprocessing.Queue()
    e = multiprocessing.Event()
    
    updateProcess = multiprocessing.Process(target = startNodaUpdateService, name = "updateProcess", args = (scheduler, newLaunchQueue, newAppsQueue, completedQueue, e))
    updateProcess.start()
        
    simulationStepCount = 0
    while True:
        if workloadGen.allJobsSubmitted() and len(scheduler.getAllApplications()) == 0:
            #notify the node update process ending
            e.set()
            break
        
        currentTime = simulationStepCount * Configuration.SIMULATION_STEP
        workloadGen.submitJobs(currentTime, scheduler)
        addedApps = scheduler.activateWaitingJobs(currentTime)
        if len(addedApps) > 0:
            #print("put new app")
            newAppsQueue.put(addedApps)
        
        while(not newLaunchQueue.empty()):
            #print("new receive container")
            newContainer = newLaunchQueue.get()
            scheduler.launchAllocatedContainer(newContainer["containerID"], newContainer["node"], newContainer["task"], newContainer["appID"])
        
        scheduler.simulate(Configuration.SIMULATION_STEP, currentTime)

        for container in scheduler._completedContaienrList:
            #print(container.getContainerID())
            completedQueue.put(container.getContainerID())
        scheduler._completedContaienrList = []
        
        simulationStepCount += 1
    
    # waiting for the end of node update process
    updateProcess.join()
        
    makespan = simulationStepCount * Configuration.SIMULATION_STEP
    finishedApp = scheduler.getFinishedAppsInfo()
    return makespan, finishedApp


def execSimulation(clusterSize, queueName, jobList, mode):
    
    if mode == "new":
        fairMakespan, fairFinishedApp = scheduling(clusterSize, queueName, jobList, 1.0)
        perfMakespan, perfFinishedApp = scheduling(clusterSize, queueName, jobList, 0.0)
    else:
        fairMakespan, fairFinishedApp = oldScheduling(clusterSize, queueName, jobList, 1.0)
        perfMakespan, perfFinishedApp = oldScheduling(clusterSize, queueName, jobList, 0.0)
    
    count = 0
    reduction = 0.0
    for k in perfFinishedApp.keys():
        count += 1
        tPerf = perfFinishedApp[k]
        tFair = fairFinishedApp[k]
        if tPerf > tFair:
            red = float(tPerf - tFair) / tFair
            reduction += red
            
    return {"fairness": float(reduction) / count, "perf": 1 - min(float(perfMakespan) / fairMakespan, 1)}


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
    if windowNum == 1:
        return A
    
    for i in range(swapNum):
        r1 = randint(0, windowNum - 1)
        r2 = r1
        while (r2 == r1):
            r2 = randint(0, windowNum - 1)
        w1 = A[r1]
        w2 = A[r2]
        
        e1 = randint(0, len(w1) - 1)
        e2 = randint(0, len(w2) - 1)
        tmp = w1[e1]
        w1[e1] = w2[e2]
        w2[e2] = tmp
        
    return A


def genWindowBasedList(A, windowSize, windowNum):
    w = []
    for i in range(windowNum):
        begin = i * windowSize
        end = min(begin + windowSize, len(A))
        w.append(A[begin:end])
        
    return w


def sortWindowBasedList(windowList):
    for i in range(len(windowList)):
        windowList[i] = sorted(windowList[i])
    
    
def genAverageEntropyByWindow(windowList):
    totalEntropy = 0
    maxEntropy = 0
    window = windowList[0]
    for w in windowList:
        entropy = Utility.calEntropyOfVectorList(w, 4)
        #print("entropy" + str(entropy))
        totalEntropy += entropy
        if entropy > maxEntropy:
            maxEntropy = entropy
            window = w
    aveEn = float(totalEntropy) / len(windowList)
    return maxEntropy, [window]
    #return aveEn


def genJobsAccordingCategoryList(categoryList, workloadSet):
    jobs = []
    codes = []
    fileName = Configuration.WORKLOAD_PATH + workloadSet
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
        code = int(items[7])
        job = JobGenerator.genComputeIntensitveJob(str(jobCount), numOfTask, memory, cpu, disk, network, taskExecTime, submissionTime)
        jobs.append(job)
        codes.append(code)
    return jobs, codes


def genJobsOfWindowList(windowList, workloadSet):
    result = []
    size = len(windowList[0])
    for w in windowList:
        if len(w) != size:
            break
        jobs, codes = genJobsAccordingCategoryList(w, workloadSet)
        entropy = Utility.calEntropyOfVectorList(codes, 4)
        result.append({"jobs": jobs, "entropy": entropy})
        
    return result


def genAveragePerfFairForWindowList(windowList, clusterSize, mode):
    #print(windowList)
    avePerf = 0
    aveFair = 0
    count = 0
    for w in windowList:
        #print(w)
        jobs = genJobsAccordingCategoryList(w)
        #print("exec simu start")
        res = execSimulation(clusterSize, "queue1", jobs, mode)
        #print("exec simu end")
        count += 1
        avePerf += float(res["perf"])
        aveFair += float(res["fairness"])
    return avePerf / count, aveFair / count


def scheduleOfJobList(jobList, clusterSize, mode):
    res = execSimulation(clusterSize, "queue1", jobList, mode)
    return res["perf"], res["fairness"]


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
        
        
def calAverageValueOfWindowBasedListForDiffClusterSize(jobCateList, workloadScale, workloadSet, windowSize, repeatNum, swapInternal, swapNum, clusterSizeList = [], mode = "new"):
    l = genJobCateList(jobCateList, workloadScale)
    windowNum = int(math.ceil(float(len(l)) / windowSize))
    w = genWindowBasedList(l, windowSize, windowNum)
    swapNumList = []
    
    swapList = []
    for i in range(swapNum + 1):
        swapList.append(swapInternal * i)
    
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
        #e, chosenWindow = genAverageEntropyByWindow(afterSwap)
        #e = genAverageEntropyByWindow(afterSwap)
        #print("entropy:" + str(float('%.1f'%e)))
        jobInfoOfWindowList = genJobsOfWindowList(afterSwap, workloadSet)
        sortedJobInfo = sorted(jobInfoOfWindowList, key = lambda k: k["entropy"], reverse = True)
        jobs = sortedJobInfo[0]["jobs"]
        e = sortedJobInfo[0]["entropy"]
        #print("entropy:" + str(float('%.1f'%e)))

        for size in clusterSizeList:
            #print("size: " + str(size))
            #print(str(datetime.now()))
            #perf, fairness = genAveragePerfFairForWindowList(afterSwap, size, mode)
            #perf, fairness = genAveragePerfFairForWindowList(chosenWindow, size, mode)
            perf, fairness = scheduleOfJobList(jobs, size, mode)
            #print(perf, fairness)
            #print(str(datetime.now()))
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
    
    '''print(str(datetime.now()))
    calAverageValueOfWindowBasedListForDiffClusterSize(24, 24, 1, 1, 10, [1,2,4,6,8,10], "new")
    print(str(datetime.now()))'''
    
    #print(str(datetime.now()))
    #calAverageValueOfWindowBasedListForDiffClusterSize(24, 24, 1, 1, 10, [1,2,4,6,8,10], "old")
    #print(str(datetime.now()))
    
    #calAverageValueOfWindowBasedListForDiffClusterSize(800, 800, 1, 60, 10, [100, 200, 400, 600, 800])
    #calAverageValueOfWindowBasedListForDiffClusterSize(500, 500, 1, 30, 10, [100, 200, 300, 400, 500])
    
    '''print("old")
    print(str(datetime.now()))
    calAverageValueOfWindowBasedListForDiffClusterSize(1000, 500, 1, 70, 10, [100, 200, 300, 400, 500], "old")
    print(str(datetime.now()))'''
    
    '''print("new")
    print(str(datetime.now()))
    calAverageValueOfWindowBasedListForDiffClusterSize(500, 500, 1, 30, 10, [100, 200, 300, 400, 500], "new")
    print(str(datetime.now()))'''
    
    #print(Utility.calEntropyOfVectorList([1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4]))
    
    #ratios = [{"window": 20, "cluster": 5}, {"window": 40, "cluster": 10}, {"window": 80, "cluster": 20}, {"window": 160, "cluster": 40}]
    '''ratios = [{"window": 160, "cluster": 40}]
    for i in ratios:
        print("window: " + str(i["window"]) + " cluster: " + str(i["cluster"]))
        print "Entropy", "Perf", "Fairness"
        calAverageValueOfWindowBasedList(i["window"], 10, 10, i["cluster"])'''
    
    #calAverageValueOfWindowBasedList(24, 24, 10, 10, 6)
    
    # experiment 1
    #calAverageValueOfWindowBasedListForDiffClusterSize(jobCateList, workloadScale, workloadSet, windowSize, repeatNum, swapInternal, swapNum, clusterSizeList = [], mode = "new"):
    
    
    print("workloadset1")
    calAverageValueOfWindowBasedListForDiffClusterSize([1, 2, 3, 4], 1000, "workloadSet1", 1000, 1, 50, 10, [200], "old")
    
    print("workloadset2")
    calAverageValueOfWindowBasedListForDiffClusterSize([1, 2, 3, 4], 2000, "workloadSet2", 2000, 1, 100, 10, [200], "old")
    
    print("workloadset3")
    calAverageValueOfWindowBasedListForDiffClusterSize([1, 2, 3, 4, 5, 6, 7, 8], 1500, "workloadSet3", 1500, 1, 120, 10, [200], "old")
    
    print("workloadset5")
    calAverageValueOfWindowBasedListForDiffClusterSize([1, 2, 3, 4, 5, 6, 7, 8], 1500, "workloadSet5", 1500, 1, 200, 10, [200], "old")
    
    print("workloadset4")
    calAverageValueOfWindowBasedListForDiffClusterSize([1, 2, 3, 4, 5, 6, 7, 8], 1000, "workloadSet4", 1000, 1, 100, 10, [200], "old")
    
    print("workloadset6")
    calAverageValueOfWindowBasedListForDiffClusterSize([1, 2, 3, 4], 1500, "workloadSet6", 1500, 1, 50, 10, [200], "old")
    
    # cluster size
    print("workloadset1, varying cluster size")
    calAverageValueOfWindowBasedListForDiffClusterSize([1, 2, 3, 4], 1000, "workloadSet1", 1000, 1, 50, 10, [100, 200, 300, 400, 500], "old")
    
    # window size
    print("workloadSet1, window size")
    print("window 500")
    calAverageValueOfWindowBasedListForDiffClusterSize([1, 2, 3, 4], 1000, "workloadSet1", 500, 1, 50, 10, [200], "old")
    
    print("window 1500")
    calAverageValueOfWindowBasedListForDiffClusterSize([1, 2, 3, 4], 1000, "workloadSet1", 1500, 1, 50, 10, [200], "old")
    
    print("window 2000")
    calAverageValueOfWindowBasedListForDiffClusterSize([1, 2, 3, 4], 1000, "workloadSet1", 2000, 1, 50, 10, [200], "old")
    
    print("window 3000")
    calAverageValueOfWindowBasedListForDiffClusterSize([1, 2, 3, 4], 1000, "workloadSet1", 3000, 1, 20, 5, [200], "old")
    
    print("window 4000")
    calAverageValueOfWindowBasedListForDiffClusterSize([1, 2, 3, 4], 1000, "workloadSet1", 4000, 1, 20, 0, [200], "old")
    
    #calAverageValueOfWindowBasedListForDiffClusterSize([1, 2, 3, 4], 24, "workloadSet", 24, 1, 2, 10, [1,2,4,6,8,10], "old")