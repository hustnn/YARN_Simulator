'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

class FSSchedulerApp(object):
    '''
    classdocs
    '''


    def __init__(self, applicationID, queue):
        '''
        Constructor
        '''
        self._requests = {}
        self._applicationID = applicationID
        self._queue = queue
        
        
    def setAppSchedulable(self, appSchedulable):
        self._appSchedulable = appSchedulable
        
        
    def updateResourceRequests(self, taskList):