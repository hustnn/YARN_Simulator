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
    #disList = genCombin(8, workloadScale * 8)
    disList = genCombin(4, workloadScale * 4)
    res = []
    for i in range(4):
        res.append([])
    
    # for accerate
    '''selectedDisList = []
    for l in disList:
        if len([i for i in l if i > 0]) == 1 or len([i for i in l if i > 0]) == 4:
            selectedDisList.append(l)'''
        
    for dis in disList:
        r = execSimulation(clusterSize, workloadScale, workloadSet, dis, numEntropy)
        #print(r)
        index = len([i for i in r["dist"] if int(i) > 0]) - 1
        if index == 0:
            for j in range(len(res)):
                res[j].append(r)
        else:
            res[index].append(r)
    
    newRes = []
    #print(len(res))
    for i in range(len(res)):
        '''print("# of res: " + str(i + 1))
        print("entropy\tdist\tfairness\tperf")'''
        newList = sorted(res[i], key=lambda k: k['entropy'])
        newRes.append(newList)
        '''for n in newList:
            print(str(n["entropy"]) + "\t" + str(n["dist"]) + "\t" + str(n["fairness"]) + "\t" + str(n["perf"]))'''
        
    # print 4 reosurce result
    '''print("entropy\tdist\tfairness\tperf")
    for n in newRes[3]:
        print(str(n["entropy"]) + "\t" + str(n["dist"]) + "\t" + str(n["fairness"]) + "\t" + str(n["perf"]))'''
    
    return newRes
             
    
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
    #[9, 6, 2, 2, 2, 1, 1, 1]  1.932931647
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
    vv1 = [v1, v2, v4, v5, v7, v8, v10, v11]
    vv2 = [v1, v1, v1, v1, v1, v1, v1, v1, v1, v2, v2, v2, v2, v2, v2, v4, v4, v5, v5, v7, v7, v8, v10, v11]
    print(len(vv2))
    e = Utility.calEntropyOfVectorList(vv2, 8)
    print(e)
    
    
def calEntropy():
    disList = genCombin(8, 3 * 8)
    res = []
    for i in range(8):
        res.append([])
    
    # for accerate
    selectedDisList = []
    for l in disList:
        if len([i for i in l if i > 0]) == 1 or len([i for i in l if i > 0]) == 8:
            selectedDisList.append(l)
    
    
    for d in selectedDisList:
        s = 0
        for i in d:
            s += i
        for i in range(len(d)):
            d[i] = float(d[i]) / s
            
        s = 0
        for p in d: 
            if p > 0:
                s += p * math.log(float(1) / p) / math.log(2)
        print(s)
            
    '''newList = []
    for d in selectedDisList:
        one = d[0] + d[1]
        two = d[2] + d[3]
        three = d[4] + d[5]
        four = d[6] + d[7]
        l = [one, two, three, four]
        newList.append(l)
        
    for d in newList:
        s = 0
        for i in d:
            s += i
        for i in range(len(d)):
            d[i] = float(d[i]) / s
            
        s = 0
        for p in d:
            if p > 0:
                s += p * math.log(float(1) / p) / math.log(2)
        print(s)'''
        
        
    
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
    
    '''sizeList = [1 * 2, 2 * 2, 4 * 2, 6 * 2, 8 * 2]
    #sizeList = [1, 2]
    res = []
    for i in range(len(sizeList)):
        res.append([])
        
    for i in range(len(sizeList)):
        #[[1], [2], [3], [4]]
        #"entropy dist fairness perf"
        l = evaluateEntropy(sizeList[i], 8, "workloadSet", 4)
        res4 = l[3]
        for j in res4:
            res[i].append(j)
    #print(res)
    for i in range(len(res[0])):
        s = str(res[0][i]["entropy"])
        for j in range(len(sizeList)):
            s += ("," + res[j][i]["fairness"])
            s += ("," + res[j][i]["perf"])
        print(s)'''
            
    #evaluateEntropy(1, 3, "workloadSetMore", 4)
    #evaluateEntropy(1, 4, "workloadSet", 4)
    
    '''scaleList = [1, 1, 1, 1]
    #sizeList = [1, 2]
    res = []
    for i in range(len(scaleList)):
        res.append([])
        
    for i in range(len(scaleList)):
        #[[1], [2], [3], [4]]
        #"entropy dist fairness perf"
        l = evaluateEntropy(1, scaleList[i], "workloadSet", 4)
        res4 = l[3]
        for j in res4:
            res[i].append(j)
    #print(res)
    for i in range(len(res[0])):
        s = str(res[0][i]["entropy"])
        for j in range(len(scaleList)):
            s += ("," + res[j][i]["fairness"])
            s += ("," + res[j][i]["perf"])
        print(s)'''
    
    #entropyTest()
    #calEntropy()
    