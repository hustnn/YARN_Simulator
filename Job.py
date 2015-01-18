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