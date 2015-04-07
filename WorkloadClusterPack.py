'''
Created on Apr 6, 2015

@author: hustnn
'''

from Cluster import Cluster
from YARNScheduler import YARNScheduler
from WorkloadGenerator import WorkloadGenerator
import Configuration
from Utility import Utility

import math

def execSimulation(clusterSize, workloadScale, workloadSet, dist, numEntropy = 4):
    cluster = Cluster(clusterSize)
    queueWorkloads = {"queue1": workloadSet}
    
    #print("fair")
    fairScheduler =  YARNScheduler(cluster, True, 1.0)
    fairScheduler.createQueue("queue1", "MULTIFAIR", True, "root")
    
    workloadGen = WorkloadGenerator(Configuration.SIMULATION_PATH, Configuration.WORKLOAD_PATH, queueWorkloads, cluster)
    '''for q in workloadGen.getQueues().keys():
        workloadGen.genWorkloadByScale(q, workloadScale)'''
    jobResVectorList = workloadGen.genWorkloadByDistribution("queue1", dist)
    entropy = Utility.calEntropyOfVectorList(jobResVectorList, numEntropy)
        
    simulationStepCount = 0
    while True:
        if workloadGen.allJobsSubmitted() and len(fairScheduler.getAllApplications()) == 0:
            break
        currentTime = simulationStepCount * Configuration.SIMULATION_STEP
        workloadGen.submitJobs(currentTime, fairScheduler)
        fairScheduler.simulate(Configuration.SIMULATION_STEP, currentTime)
        simulationStepCount += 1
        
    fairMakespan = simulationStepCount * Configuration.SIMULATION_STEP
    fairFinishedApp = fairScheduler.getFinishedAppsInfo()
    
    #print("perf")
    cluster = Cluster(clusterSize)
    perfScheduler = YARNScheduler(cluster, True, 0.0)
    perfScheduler.createQueue("queue1", "MULTIFAIR", True, "root")
    
    workloadGen = WorkloadGenerator(Configuration.SIMULATION_PATH, Configuration.WORKLOAD_PATH, queueWorkloads, cluster)
    '''for q in workloadGen.getQueues().keys():
        workloadGen.genWorkloadByScale(q, workloadScale)'''
    workloadGen.genWorkloadByDistribution("queue1", dist)
        
    simulationStepCount = 0
    while True:
        if workloadGen.allJobsSubmitted() and len(perfScheduler.getAllApplications()) == 0:
            break
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
        
    '''print("fair")    
    for k, v in fairFinishedApp.items():
        print(k, v)
    
    print("perf")
    for k, v in perfFinishedApp.items():
        print(k, v)'''
            
    '''print(clusterSize, workloadScale, workloadSet, dist)
    print("Entropy: " + str(entropy))
    print("slowdown: " + str(reduction / count))
    print(perfMakespan, fairMakespan)
    print("perf reduction: " + str(1 - float(perfMakespan) / fairMakespan))'''
    
    return {"entropy": entropy, "dist": dist, "fairness": str(reduction / count), "perf": str(1 - min(float(perfMakespan) / fairMakespan, 1))}
    
    
def evaluateEntropy(clusterSize, workloadScale, workloadSet, numEntropy = 4):
    disList = genCombin(4, workloadScale * 1)
    res = [[]] * 4
    for dis in disList:
        r = execSimulation(clusterSize, workloadScale, workloadSet, dis)
        index = sum(i > 0 for i in r["dist"]) - 1
        res[index].append(r)
        
    for i in range(len(res)):
        print("# of res: " + str(i + 1))
        print("entropy dist fairness perf")
        print(r[i])
        newList = sorted(r[i], key=lambda k: k['entropy'])
        for n in newList:
            print(str(n["entropy"]) + " " + str(n["dist"]) + " " + str(n["fairness"]) + " " + str(n["perf"]))
             
    
def genCombinations(categoryNum, totalNum):
    res = []
    for i in range(1, categoryNum):
        #print("range" + str(i))
        upper = totalNum
        lower = int(math.ceil(float(totalNum) / (i + 1)))
        n = upper
        while n >= lower:
            d = [0] * categoryNum
            d[0] = n
            left = upper - n
            index = 0
            while left > 0:
                d[1 + index] = d[1 + index] + 1
                index = (index + 1) % i
                left -= 1
            n -= 1
            if tuple(d) not in res:
                res.append(tuple(d))
    
    return [list(i) for i in res]


def genCombin(categoryNum, totalNum):
    res = []
    for i in range(1, categoryNum + 1):
        com = genCom(totalNum, totalNum, i)
        com.reverse()
        for c in com:
            res.append(c)
        
    result = []
    for r in res:
        if len(r) < categoryNum:
            l = categoryNum - len(r)
            result.append(r + [0] * l)
        else:
            result.append(r)
            
    return result


def genCom(upper, cur, left):
    if left == 1:
        return [[cur]]
    else:
        res = []
        u = min(upper, cur - left + 1)
        l = int(math.ceil(float(cur) / left))
        
        for i in range(l, u + 1):
            c = [i]
            n = genCom(i, cur - i, left - 1)
            for r in n:
                res.append(c + r)
                
        return res
    
    
def entropyTest():
    v1 = [6, 1, 1, 1]
    v2 = [4, 1, 1, 1]
    v3 = [2, 1, 1, 1]
    v4 = [1, 6, 1, 1]
    v5 = [1, 4, 1, 1]
    v6 = [1, 2, 1, 1]
    v7 = [1, 1, 6, 1]
    v8 = [1, 1, 4, 1]
    v9 = [1, 1, 2, 1]
    v10 = [1, 1, 1, 6]
    v11 = [1, 1, 1, 4]
    v12 = [1, 1, 1, 2]
    vv1 = [v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12]
    vv2 = [v1, v3, v5, v7]
    Utility.calEntropyOfVectorList(vv1, 4)
    

if __name__ == '__main__':
    '''print("fixed cluster size:")
    for scale in range(1, 11):
        print("workload scale: " + str(scale))
        execSimulation(50, scale)
        print("\n")'''
        
    '''print("fixed workload scale")
    for size in [1, 2, 3, 4, 5, 6, 7, 8]:
        print("cluster size: " + str(size))
        execSimulation(size, 1)
        print("\n")'''
    
    '''scale = 4
    disList = genCombin(4, scale * 4)
    for dis in disList:
        execSimulation(1, scale, "workloadSet", dis)
        print("\n")'''
        
    #entropyTest()
    evaluateEntropy(1, 4, "workloadSet")
    