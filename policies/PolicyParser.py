'''
Created on Jan 15, 2015

@author: niuzhaojie
'''


from MultipleResourceFitnessPolicy import MultipleResourceFitnessPolicy
from FairSharePolicy import FairSharePolicy
from DominantResourceFairnessPolicy import DominantResourceFairnessPolicy
from MultipleResourceFairnessPolicy import MultipleResourceFairnessPolicy
from MaxResourceEntropyPolicy import MaxResourceEntropyPolicy
from FifoPolicy import FifoPolicy

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
            if (text == FifoPolicy.NAME.lower()):
                policyInstance = FifoPolicy()
            elif (text == FairSharePolicy.NAME.lower()):
                policyInstance = FairSharePolicy()
            elif (text == DominantResourceFairnessPolicy.NAME.lower()):
                policyInstance = DominantResourceFairnessPolicy()
            elif (text == MultipleResourceFitnessPolicy.NAME.lower()):
                policyInstance = MultipleResourceFitnessPolicy()
            elif (text == MultipleResourceFairnessPolicy.NAME.lower()):
                policyInstance = MultipleResourceFairnessPolicy()
            elif (text == MaxResourceEntropyPolicy.NAME.lower()):
                policyInstance = MaxResourceEntropyPolicy()
            else:
                policyInstance = FairSharePolicy()
                
            cls.instances[text] = policyInstance
        
        policyInstance.initialize(capacity)
        return policyInstance
    