'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

from JobGenerator import JobGenerator

class WorkloadGenerator(object):
    '''
    classdocs
    '''


    def __init__(self, tracePath, workloadPath, queueWorkloadDict, cluster):
        '''
        Constructor
        '''
        self._tracePath = tracePath
        self._workloadPath = workloadPath
        self._cluster = cluster
        self._queues = {}
        for k, v in queueWorkloadDict.items():
            self._queues[k] = {"traceFile": v, "totalJobs": 0, "submittedJobs": 0, "jobList": []}
        #for queue in self._queues.keys():
        #    self.genWorkload(queue)
            
            
    def getQueues(self):
        return self._queues
            
            
    def submitJobs(self, currentTime, scheduler):
        for k, v in self._queues.items():
            while(v["submittedJobs"] < v["totalJobs"] and (v["jobList"][v["submittedJobs"]]).getSubmissionTime() <= currentTime):
                job = v["jobList"][v["submittedJobs"]]
                v["submittedJobs"] += 1
                scheduler.submitJob(job, k)
                
                
    def allJobsSubmitted(self):
        ret = True
        for info in self._queues.values():
            if info["submittedJobs"] < info["totalJobs"]:
                ret = False
                break
            
        return ret
    
    
    def genWorkloadByList(self, queue, jobs):
        for job in jobs:
            self._queues[queue]["totalJobs"] += 1
            self._queues[queue]["jobList"].append(job)
    
    
    def genWorkloadByScale(self, queue, scale = 1):
        fileName = self._workloadPath + self._queues[queue]["traceFile"]
        f = open(fileName, "r")
        lines = f.readlines()
        f.close()
        jobCount = 0
        for line in lines:
            items = line.split(",")
            numOfTask = int(items[0])
            taskExecTime = int(items[1])
            submissionTime = int(items[2])
            memory = int(items[3])
            cpu = int(items[4])
            disk = int(items[5])
            network = int(items[6])
            for i in range(scale):
                jobID = jobCount + 1
                jobCount += 1
                job = JobGenerator.genComputeIntensitveJob(str(jobID), numOfTask, memory, cpu, disk, network, taskExecTime, submissionTime)
                if job != None:
                    self._queues[queue]["totalJobs"] += 1
                    self._queues[queue]["jobList"].append(job)
            
        
    def genWorkloadByDistribution(self, queue, dist):
        fileName = self._workloadPath + self._queues[queue]["traceFile"]
        f = open(fileName, "r")
        lines = f.readlines()
        f.close()
        
        jobCount = 0
        jobResVector = []
        for i in range(len(dist)):
            num = dist[i]
            for j in range(num):
                line = lines[i]
                items = line.split(",")
                numOfTask = int(items[0])
                taskExecTime = int(items[1])
                submissionTime = int(items[2])
                memory = int(items[3])
                cpu = int(items[4])
                disk = int(items[5])
                network = int(items[6])
                jobCount += 1 
                job = JobGenerator.genComputeIntensitveJob(str(jobCount), numOfTask, memory, cpu, disk, network, taskExecTime, submissionTime)
                jobResVector.append(job.getResourceVector())
                if job!= None:
                    self._queues[queue]["totalJobs"] += 1
                    self._queues[queue]["jobList"].append(job)
                         
        return jobResVector
                    
            
    def genWorkload(self, queue):
        #fileName = self._tracePath + self._queues[queue]["traceFile"]
        fileName = self._workloadPath + self._queues[queue]["traceFile"]
        f = open(fileName, "r")
        lines = f.readlines()
        job = None
        
        for line in lines:
            items = line.split(",")
            # 1. genComputeIntensitveJob(jobID, numOfTask, memory, cpu, disk, network, execTime, submissionTime)
            # 2. genMapOnlyJob(jobID, inputFile, memory, cpu, disk, network, submissionTime)
            # 3. genMapReduceJob(jobID, inputFile, mapMemory, mapCPU, mapDisk, mapNetwork, numOfReduce, redMemory, redCPU, redDisk, redNetwork, submissionTime)
            if items[0] == "compute":
                jobID = queue + "-" + str(self._queues[queue]["totalJobs"]) + "-compute"
                numOfTask = int(items[1])
                memory = int(items[2])
                cpu = int(items[3])
                disk = int(items[4])
                network = int(items[5])
                execTime = int(items[6])
                submissionTime = int(items[7])
                job = JobGenerator.genComputeIntensitveJob(jobID, numOfTask, memory, cpu, disk, network, execTime, submissionTime)
            elif items[0] == "map":
                jobID = queue + "-" + str(self._queues[queue]["totalJobs"]) + "-map"
                inputFile = self._cluster.getFile(items[1])
                memory = int(items[2])
                cpu = int(items[3])
                disk = int(items[4])
                network = int(items[5])
                submissionTime = int(items[6])
                job = JobGenerator.genMapOnlyJob(jobID, inputFile, memory, cpu, disk, network, submissionTime)
            elif items[0] == "mapReduce":
                jobID = queue + "-" + str(self._queues[queue]["totalJobs"]) + "-mapreduce"
                inputFile = self._cluster.getFile(items[1])
                mapMemory = int(items[2])
                mapCPU = int(items[3])
                mapDisk = int(items[4])
                mapNetwork = int(items[5])
                numOfReduce = int(items[6])
                redMemory = int(items[7])
                redCPU = int(items[8])
                redDisk = int(items[9])
                redNetwork = int(items[10])
                submissionTime = int(items[11])
                job = JobGenerator.genMapReduceJob(jobID, inputFile, mapMemory, mapCPU, mapDisk, mapNetwork, numOfReduce, redMemory, redCPU, redDisk, redNetwork, submissionTime)
                
            if job != None:
                self._queues[queue]["totalJobs"] += 1
                self._queues[queue]["jobList"].append(job)
            
        f.close()
        