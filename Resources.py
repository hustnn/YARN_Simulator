'''
Created on Jan 11, 2015

@author: zhaojie
'''

from Resource import Resource

class Resources(object):
    '''
    classdocs
    '''
    @staticmethod
    def createResource(memory, cpu, disk, network):
        return Resource(memory, cpu, disk, network)
        