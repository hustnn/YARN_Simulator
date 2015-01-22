'''
Created on Jan 9, 2015

@author: niuzhaojie
'''
from SchedulableStatus import SchedulableStatus

class Job(object):
    '''
    classdocs
    '''


    def __init__(self, jobID, submissionTime):
        '''
        Constructor
        '''
        self._jobID = jobID
        self._status = SchedulableStatus.WAITING
        self._taskList = []
        self._submissionTime = submissionTime
        self._startTime = None
        self._finishTime = None
        
        
    def updateStatus(self, status):
        self._status = status
        
        
    def addTask(self, task):
        self._taskList.append(task)
        
        
    def getJobID(self):
        return self._jobID
    
    
    def getTaskList(self):
        return self._taskList
    
    
    def getSubmissionTime(self):
        return self._submissionTime
    
    
    def setStartTime(self, t):
        self._startTime = t
        
        
    def setFinishTime(self, t):
        self._finishTime = t
        
        
    def getFinishTime(self):
        return self._finishTime
        
        
    def activeAllTasks(self):
        for task in self._taskList:
            if task.getStatus() == SchedulableStatus.WAITING and len(task.getParents()) == 0:
                task.updateStatus(SchedulableStatus.RUNNABLE)
            elif task.getStatus() == SchedulableStatus.WAITING and len(task.getParents()) > 0:
                task.updateStatus(SchedulableStatus.PENDING)
                
                
    def getNumofUnFinishedTasks(self):
        count = 0
        for task in self._taskList:
            if task.getStatus() != SchedulableStatus.FINISHING:
                count += 1
        return count
                
                
    def allTasksFinished(self):
        ret = True
        for task in self._taskList:
            if task.getStatus() != SchedulableStatus.FINISHING:
                ret = False
                break
            
        return ret
                
                
    def updateStatusOfPendingTasks(self):
        for task in self._taskList:
            if task.getStatus() == SchedulableStatus.PENDING and task.parentsAllFinished():
                task.updateStatus(SchedulableStatus.RUNNABLE)