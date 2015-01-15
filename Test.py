'''
Created on Jan 15, 2015

@author: niuzhaojie
'''

class SClass(object):
    '''
    classdocs
    '''
    list = []

    @classmethod
    def hello(cls, policy):
        cls.list.append(policy)
        return policy
    