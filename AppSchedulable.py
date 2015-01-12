'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

from AppSchedulable import AppSchedulable

class AppSchedulable(AppSchedulable):
    '''
    classdocs
    '''


    def __init__(self, app, queue):
        '''
        Constructor
        '''
        self._app = app
        self._queue = queue
        
        
    def getApp(self):
        return self._app