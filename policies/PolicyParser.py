'''
Created on Jan 15, 2015

@author: niuzhaojie
'''


from MultipleResourceFitnessPolicy import MultipleResourceFitnessPolicy
from FairSharePolicy import FairSharePolicy
from DominantResourceFairnessPolicy import DominantResourceFairnessPolicy
from MultipleResourceFairnessPolicy import MultipleResourceFairnessPolicy


class PolicyParser(object):
    '''
    classdocs
    '''
    instances = {}
    
    @classmethod
    def getInstance(cls, policy, capacity):
        text = policy.lower()
        policyInstance = cls.instances.get(text)
        if policyInstance == None:
            if (text == FairSharePolicy.NAME.lower()):
                policyInstance = FairSharePolicy()
            elif (text == DominantResourceFairnessPolicy.NAME.lower()):
                policyInstance = DominantResourceFairnessPolicy()
            elif (text == MultipleResourceFitnessPolicy.NAME.lower()):
                policyInstance = MultipleResourceFitnessPolicy()
            elif (text == MultipleResourceFairnessPolicy.NAME.lower()):
                policyInstance = MultipleResourceFairnessPolicy()
            else:
                policyInstance = FairSharePolicy()
                
            cls.instances[text] = policyInstance
        
        policyInstance.initialize(capacity)
        return policyInstance
    