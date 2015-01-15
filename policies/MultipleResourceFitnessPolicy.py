'''
Created on Jan 8, 2015

@author: niuzhaojie
'''
import sys

sys.path.append('../')

from SchedulingPolicy import SchedulingPolicy

class MultipleResourceFitnessPolicy(SchedulingPolicy):
    '''
    classdocs
    '''
    NAME = "MRF"

    def __init__(self):
        '''
        Constructor
        '''
        