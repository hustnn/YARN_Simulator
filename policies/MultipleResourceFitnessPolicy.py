'''
Created on Jan 8, 2015

@author: niuzhaojie
'''
import sys


sys.path.append('../')

from SchedulingPolicy import SchedulingPolicy
from Utility import Utility
from ComputeFairShares import ComputeFairShares
from ResourceType import ResourceType


class MultipleResourceFitnessPolicy(SchedulingPolicy):
    '''
    classdocs
    '''
    NAME = "MRF"

    
    def compare(self, s1, s2):
        s1Fitness = s1.getMultiResFitness()
        s2Fitness = s2.getMultiResFitness()
        
        res = s2Fitness - s1Fitness
        
        #if abs(res) <= 0.01:
        #    res = 0
        
        return Utility.sign(res)
    
    
    def getComparator(self):
        return self.compare
    
    
    def computeShares(self, schedulables, totalResources):
        ComputeFairShares.computeShares(schedulables, totalResources, ResourceType.MEMORY)
        ComputeFairShares.computeShares(schedulables, totalResources, ResourceType.CPU)
        ComputeFairShares.computeShares(schedulables, totalResources, ResourceType.DISK)
        ComputeFairShares.computeShares(schedulables, totalResources, ResourceType.NETWORK)
        