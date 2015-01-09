'''
Created on Jan 9, 2015

@author: niuzhaojie
'''
from Task import Task

class IOTask(Task):
    '''
    classdocs
    '''


    def __init__(self, taskID, priority, resourceRequest, blockSize):
        '''
        Constructor
        '''
        self.__init__(taskID, priority, resourceRequest)
        self._blockSize = blockSize