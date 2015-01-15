'''
Created on Jan 16, 2015

@author: zhaojie
'''

class Utility(object):
    '''
    classdocs
    '''

    @staticmethod
    def sign(num):
        if num == 0:
            return 0
        elif num > 0:
            return 1
        else:
            return -1
        
    
    @staticmethod
    def compareTo(str1, str2):
        if str1 == str2:
            return 0
        elif str1 < str2:
            return -1
        else:
            return 1
        