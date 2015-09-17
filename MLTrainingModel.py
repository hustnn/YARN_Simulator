'''
Created on May 5, 2015

@author: hustnn
'''



from Utility import Utility
from Cluster import Cluster
from YARNScheduler import YARNScheduler
from WorkloadGenerator import WorkloadGenerator
from JobGenerator import JobGenerator

import Configuration

import math
import copy
from random import randint
import time

def genJobCategoryList(num, prob):
    total = num
    one = int(num * prob)
    total -= one
    two = int(min(total, math.ceil(float(total) / 3)))
    total -= two
    three = int(min(total, math.ceil(float(total) / 2)))
    total -= three
    four = total
    res = []
    res.extend([1] * one)
    res.extend([2] * two)
    res.extend([3] * three)
    res.extend([4] * four)
    return res


def getEntropyOfList(v, percentage = 1.0):
    length = int(len(v) * float(percentage))
    entropy = Utility.calEntropyOfVectorList(v[:length]) 
    return float("{0:.1f}".format(entropy))


def JobListSizeToClusterSize(jobSize, clusterSize):
    ratio = int(math.ceil(float(jobSize) / clusterSize))
    if ratio > 20:
        ratio = 20
        
    return ratio


def genDominantDegree(num):
    if num == 2:
        return "s"
    elif num == 4:
        return "m"
    elif num == 6:
        return "l"
    
    
def maxContineousDistance(v):
    lastItem = None
    contineousCount = 1
    maxCount = contineousCount
    for i in v:
        if i != lastItem:
            if contineousCount > maxCount:
                maxCount = contineousCount
            contineousCount = 1
        else:
            contineousCount += 1
            
        lastItem = i
    if contineousCount > maxCount:
        maxCount = contineousCount
    return maxCount


def contineousDisToClusterSize(contineousDis, clusterSize):
    ratio = int(math.ceil(float(contineousDis) / clusterSize))
    if ratio > 20:
        ratio = 20
        
    return ratio


def calDistributionPosition(jobs, target, percentage):
    numOfTarget = len([i for i in jobs if i == target])
    num = int(float(numOfTarget) * percentage)
    count = 0
    position = 0
    for i in jobs:
        if count == num:
            break
        if i == target:
            count += 1
            
        position += 1
        
    return int(float(position) / len(jobs) * 10)


def genJobsAccordingCategoryList(jobCategoryList, dominantRes, dominantDegree):
    jobs = []
    jobCount = 0
    for category in jobCategoryList:
        resVector = [1, 1, 1, 1]
        resVector[category - 1] = dominantRes[category - 1][dominantDegree]
        job = JobGenerator.genComputeIntensitveJob(str(jobCount), 200, resVector[0], resVector[1], resVector[2], resVector[3], 1, 0)
        jobs.append(job)
        jobCount += 1
        
    return jobs


def swapJobs(jobs, swapNum):
    copyJobs = jobs[:]
    for i in range(swapNum):
        r1 = randint(0, len(copyJobs) - 1)
        r2 = r1
        while (r2 == r1):
            r2 = randint(0, len(copyJobs) - 1)
            
        tmp = copyJobs[r1]
        copyJobs[r1] = copyJobs[r2]
        copyJobs[r2] = tmp
    return copyJobs


def fairAllocation(clusterSize, queueName, jobList):
    cluster = Cluster(clusterSize)

    scheduler =  YARNScheduler(cluster, True, 1)
    scheduler.createQueue("queue1", "MULTIFAIR", True, "root")
    
    workloadGen = WorkloadGenerator(Configuration.SIMULATION_PATH, Configuration.WORKLOAD_PATH, {queueName: jobList}, cluster)
    workloadGen.genWorkloadByList(queueName, copy.deepcopy(jobList))
    workloadGen.submitJobs(0, scheduler)
    scheduler.activateWaitingJobs(0)
    
    appsDict = scheduler.resourceAllocateSimulate()
    return appsDict


def perfAllocation(clusterSize, queueName, jobList):
    cluster = Cluster(clusterSize)

    scheduler =  YARNScheduler(cluster, True, 0)
    scheduler.createQueue("queue1", "MULTIFAIR", True, "root")
    
    workloadGen = WorkloadGenerator(Configuration.SIMULATION_PATH, Configuration.WORKLOAD_PATH, {queueName: jobList}, cluster)
    workloadGen.genWorkloadByList(queueName, copy.deepcopy(jobList))
    workloadGen.submitJobs(0, scheduler)
    scheduler.activateWaitingJobs(0)
    
    appsDict = scheduler.resourceAllocateSimulate()
    return appsDict


def calFairnessLoss(appsFair, appsPerf):
    totalNumOfApps = len(appsFair)
    numOfUnfairApps = 0
    fairnessLoss = 0
    for appID in appsFair.keys():
        if appsPerf[appID] < appsFair[appID]:
            numOfUnfairApps += 1
            fairnessLoss += float(appsFair[appID] - appsPerf[appID]) / appsFair[appID]
            
    '''if numOfUnfairApps == 0:
        return 0, 0, 0
    else:
        return int(float(numOfUnfairApps) / totalNumOfApps * 100), int(float(fairnessLoss) / numOfUnfairApps * 100), int(float(fairnessLoss) / totalNumOfApps * 100)'''
    if numOfUnfairApps == 0:
        return 0
    else:
        return int(math.ceil(float(fairnessLoss) / totalNumOfApps * 100))
    
    
def normalizeFairnessLoss(loss):
    normLoss = int(math.ceil(float(loss) / 2))
    return normLoss


if __name__ == '__main__':
    dominantRes = [[2, 4, 6], [2, 4, 6], [2, 4, 6], [2, 4, 6]]
    
    '''
    numOfJobs = 10
    clusterSize = 10
    
    jobCategoryList = genJobCategoryList(numOfJobs, 0.5)
    categoryList = swapJobs(jobCategoryList, 5)
    
    jobs = genJobsAccordingCategoryList(categoryList, dominantRes, 2)
    appsFair = fairAllocation(clusterSize, "queue1", jobs)
    appsPerf = perfAllocation(clusterSize, "queue1", jobs)
    percentageLoss, aveLoss = calFairnessLoss(appsFair, appsPerf)
    print(percentageLoss, aveLoss)'''
    
    numOfJobList = [100, 200, 300, 400, 500]
    distributionList = [1, 0.9, 0.8, 0.6, 0.5, 0.25]
    clusterSizeList = [50, 100, 200]
    dominantDegreeList = [0, 1, 2]
    swapNumList = [0, 10, 50, 100, 200]
    percentageLossList = [0, 5, 10, 15, 20, 25, 30, 35, 40]
    aveLossList = [0, 5, 10, 15, 20, 25, 30, 35, 40]
    
    filename = Configuration.WORKLOAD_PATH + "trainingModel"
    f = open(filename, "w")
    f.write("entropy,entropy80,entropy60,entropy40,entropy20,normJobSize,normCon,disPos1,disPos2,disPos3,dominantDegree,predAveLoss,aveLoss,policy\n")
    
    print("begin...")
    for jobNum in numOfJobList:
        for prob in distributionList:
            jobCategoryList = genJobCategoryList(jobNum, prob)
            entropy = getEntropyOfList(jobCategoryList)
            entropy20 = getEntropyOfList(jobCategoryList, 0.2)
            entropy40 = getEntropyOfList(jobCategoryList, 0.4)
            entropy60 = getEntropyOfList(jobCategoryList, 0.6)
            entropy80 = getEntropyOfList(jobCategoryList, 0.8)
            for clusterSize in clusterSizeList:
                normJobSize = JobListSizeToClusterSize(jobNum, clusterSize)
                for swapNum in swapNumList:
                    jobCategoryListAfterSwap = swapJobs(jobCategoryList, swapNum)
                    maxContineousCount = maxContineousDistance(jobCategoryListAfterSwap)
                    normContineousCount = contineousDisToClusterSize(maxContineousCount, clusterSize)
                    disPos1 = calDistributionPosition(jobCategoryListAfterSwap, 1, 0.3)
                    disPos2 = calDistributionPosition(jobCategoryListAfterSwap, 1, 0.5)
                    disPos3 = calDistributionPosition(jobCategoryListAfterSwap, 1, 0.7)
                    for dominantDegree in dominantDegreeList:
                        jobs = genJobsAccordingCategoryList(jobCategoryListAfterSwap, dominantRes, dominantDegree)
                        appsFair = fairAllocation(clusterSize, "queue1", jobs)
                        appsPerf = perfAllocation(clusterSize, "queue1", jobs)
                        aveLoss = calFairnessLoss(appsFair, appsPerf)
                        #normAveLoss = normalizeFairnessLoss(aveLoss)
                        for al in aveLossList:
                            #nal = normalizeFairnessLoss(al)
                            if aveLoss <= al:
                                policy = "P"
                            else:
                                policy = "F"
                            feature = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % (str(entropy), str(entropy80), str(entropy60), str(entropy40), str(entropy20), 
                                                                                             str(normJobSize), str(normContineousCount), str(disPos1), str(disPos2),str(disPos3), 
                                                                                             str(dominantDegree), str(aveLoss), str(al), str(policy))
                            f.write(feature)
                            f.flush()
                        '''percentageLoss, aveLoss = calFairnessLoss(appsFair, appsPerf)
                        normPercentageLoss = normalizeFairnessLoss(percentageLoss)
                        normAveLoss = normalizeFairnessLoss(aveLoss)
                        for pl in percentageLossList:
                            for al in aveLossList:
                                npl = normalizeFairnessLoss(pl)
                                nal = normalizeFairnessLoss(al)
                                if normPercentageLoss <= npl and normAveLoss <= nal:
                                    policy = "P"
                                else:
                                    policy = "F"
                                feature = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % (str(entropy), str(entropy80), str(entropy60), str(entropy40), str(entropy20), 
                                                                                                 str(normJobSize), str(normContineousCount), str(disPos1), str(disPos2),str(disPos3), 
                                                                                                 str(dominantDegree), str(normPercentageLoss), str(normAveLoss), str(npl), str(nal), str(policy))
                                f.write(feature)'''
                                
    f.close()
    print("end")