'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

from FSQueue import FSQueue
from Resources import Resources

class FSParentQueue(FSQueue):
    '''
    classdocs
    '''

    def __init__(self, name, parent):
        '''
        Constructor
        '''
        super(FSParentQueue, self).__init__(name, parent)
        self._childQueues = []
        self._demand = Resources.createResource(0, 0, 0, 0)
        
    
    def addChildQueue(self, child):
        self._childQueues.append(child)