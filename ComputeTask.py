'''
Created on Jan 9, 2015

@author: niuzhaojie
'''
from Task import Task

class ComputeTask(Task):
    '''
    classdocs
    '''


    def __init__(self, taskID, priority, resource, execTime):
        '''
        Constructor
        '''
        super(ComputeTask, self).__init__(taskID, priority, resource)
        self._execTime = execTime
        self._workload = execTime
        
        
    def getWorkload(self):
        return self._workload
        
        
    def schedule(self, t):
        if self._workload < t:
            self._workload = 0
        else:
            self._workload -= t
        
        