'''
Created on Jan 8, 2015

@author: niuzhaojie
'''
import sys

sys.path.append('../')

from SchedulingPolicy import SchedulingPolicy

class LTDominantResourceFairnessPolicy(SchedulingPolicy):
    '''
    classdocs
    '''
    NAME = "LTDRF"

    def __init__(self):
        '''
        Constructor
        '''
        