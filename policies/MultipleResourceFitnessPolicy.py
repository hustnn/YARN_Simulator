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
        
        return Utility.sign(s2Fitness - s2Fitness)
    
    
    def getComparator(self):
        return self.compare
    
    
    def computeShares(self, schedulables, totalResources):
        ComputeFairShares.computeShares(schedulables, totalResources, ResourceType.MEMORY)
        ComputeFairShares.computeShares(schedulables, totalResources, ResourceType.CPU)
        