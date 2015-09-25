'''
Created on Sep 24, 2015

@author: hustnn
'''

import math


def isVectorComplementary(vec1, vec2):
    if vec1.index(max(vec1)) == vec2.index(max(vec2)):
        return False
    else:
        return True
    
    
def vectorLen(v):
    s = 0
    for i in v:
        s += i * i
        
    return math.sqrt(s)


def dotProduct(v1, v2):
    s = 0
    for i in range(len(v1)):
        s += v1[i] * v2[i]
        
    return s

    
def calConsin(vec1, vec2):
    return float(dotProduct(vec1, vec2)) / (vectorLen(vec1) * vectorLen(vec2))
    

if __name__ == '__main__':
    #print(calConsin([10, 1, 1], [10, 1, 1]))
    print(calConsin([10, 1, 1], [1, 10, 1]))
    print(calConsin([10, 1, 10], [1, 10, 1]))
    #print(calConsin([10, 1, 5], [1, 10, 5]))
    print(calConsin([10, 10, 1], [1, 10, 1]))
    
    print(calConsin([10, 1, 1], [1, 1, 1]))
    print(calConsin([1, 10, 1], [1, 1, 1]))
    
    print(calConsin([10, 1, 10], [1, 1, 1]))
    print(calConsin([1, 10, 1], [1, 1, 1]))
    
    print(calConsin([10, 10, 1], [1, 1, 1]))
    print(calConsin([1, 10, 1], [1, 1, 1]))
    
    