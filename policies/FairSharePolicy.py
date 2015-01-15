'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

import sys

sys.path.append('../')


from SchedulingPolicy import SchedulingPolicy
from Utility import Utility

class FairSharePolicy(SchedulingPolicy):
    '''
    classdocs
    '''
    NAME = "fair"

    
    @staticmethod
    def compare(s1, s2):
        useToWeightRatio1 = s1.getResourceUsage().getMemory() / s1.getWeight()
        useToWeightRatio2 = s2.getResourceUsage().getMemory() / s2.getWeight()
        
        s1Needy = s1.getDemand().getMemory() > s1.getResourceUsage().getMemory()
        s2Needy = s2.getDemand().getMemory() > s2.getResourceUsage().getMemory()
        
        res = 0
        
        if s1Needy and not s2Needy:
            res = -1
        elif s2Needy and not s1Needy:
            return 1
        elif s1Needy and s2Needy:
            res = Utility.sign(useToWeightRatio1 - useToWeightRatio2)
            
        if res == 0:
            res = Utility.sign(s1.getStartTime() - s2.getStartTime())
            if res == 0:
                res = Utility.compareTo(s1.getName(), s2.getName())
                
        return res