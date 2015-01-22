'''
Created on Jan 15, 2015

@author: niuzhaojie
'''
from MultipleResourceFairnessPolicy import MultipleResourceFairnessPolicy
from FairSharePolicy import FairSharePolicy
from DominantResourceFairnessPolicy import DominantResourceFairnessPolicy


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
            elif (text == MultipleResourceFairnessPolicy.NAME.lower()):
                policyInstance = MultipleResourceFairnessPolicy()
            else:
                policyInstance = FairSharePolicy()
                
            cls.instances[text] = policyInstance
        
        policyInstance.initialize(capacity)
        return policyInstance
    