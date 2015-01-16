'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

from Resource import Resource
from Test import SClass 
import Configuration

from policies.FairSharePolicy import FairSharePolicy


class A(object):
    def f(self, s1 ,s2):
        if s1 < s2:
            return -1
        else:
            return 1
    
    
    def a(self):
        return self.f

if __name__ == '__main__':
    c = A()
    a = [1, 3, 2]
    a.sort(c.a())
    print(a)
    