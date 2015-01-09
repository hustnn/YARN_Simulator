'''
Created on Jan 9, 2015

@author: niuzhaojie
'''
from SchedulableStatus import SchedulableStatus

class Job(object):
    '''
    classdocs
    '''


    def __init__(self, jobID):
        '''
        Constructor
        '''
        self._jobID = jobID
        self._status = SchedulableStatus.WAITING
        