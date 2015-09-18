'''
Created on Sep 18, 2015

@author: hustnn
'''

import sys

sys.path.append('../')

from SchedulingPolicy import SchedulingPolicy
from Utility import Utility
from ComputeFairShares import ComputeFairShares
from ResourceType import ResourceType

class FifoPolicy(SchedulingPolicy):
    '''
    classdocs
    '''


    NAME = "FIFO"
    
    
    def compare(self, s1, s2):
        res = Utility.sign(s1.getStartTime() - s2.getStartTime())
        if res == 0:
            res = Utility.compareTo(s1.getName(), s2.getName())
            
        return res
    
    
    def getComparator(self):
        return self.compare
    
    
    def computeShares(self, schedulables, totalResources):
        ComputeFairShares.computeShares(schedulables, totalResources, ResourceType.MEMORY)