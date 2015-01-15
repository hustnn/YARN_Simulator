'''
Created on Jan 15, 2015

@author: niuzhaojie
'''

from FairSharePolicy import FairSharePolicy
from DominantResourceFairnessPolicy import DominantResourceFairnessPolicy
from LTDominantResourceFairnessPolicy import LTDominantResourceFairnessPolicy
from MultipleResourceFitnessPolicy import MultipleResourceFitnessPolicy


class PolicyParser(object):
    '''
    classdocs
    '''
    instances = {}
    
    @classmethod
    def getInstance(cls, policy):
        text = policy.lower()
        policyInstance = cls.instances.get(text)
        if policyInstance == None:
            if (text == FairSharePolicy.NAME.lower()):
                policyInstance = FairSharePolicy()
            elif (text == DominantResourceFairnessPolicy.NAME.lower()):
                policyInstance = DominantResourceFairnessPolicy()
            elif (text == LTDominantResourceFairnessPolicy.NAME.lower()):
                policyInstance = LTDominantResourceFairnessPolicy()
            elif (text == MultipleResourceFitnessPolicy.NAME.lower()):
                policyInstance = MultipleResourceFitnessPolicy()
            else:
                policyInstance = FairSharePolicy()
                
            cls.instances[text] = policyInstance
            
        return policyInstance