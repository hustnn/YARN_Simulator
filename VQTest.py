'''
Created on Mar 14, 2015

@author: niuzhaojie
'''
from numpy import array
from scipy.cluster.vq import vq, kmeans, whiten, kmeans2
import math

def calEntropy(probDict):
    sum = 0
    for p in probDict.values(): 
        sum += p * math.log(float(1) / p) / math.log(2)
        
    return sum

if __name__ == '__main__':
    features1  = array([[ 12288, 1, 8, 8],
                       [ 12288, 1, 8, 8],
                       [ 12288, 1, 8, 8],
                       [ 12288, 1, 8, 8],
                       [ 12288, 1, 8, 8],
                       [ 12288, 1, 8, 8],
                       [ 2048, 6, 8, 8],
                       [ 2048, 6, 8, 8],
                       [ 2048, 6, 8, 8],
                       [ 2048, 6, 8, 8],
                       [ 2048, 6, 8, 8],
                       [ 2048, 6, 8, 8]])
    
    features2  = array([[ 12288, 1, 8, 8],
                       [ 2048, 6, 8, 8],
                       [ 2048, 1, 128, 8],
                       [ 2048, 1, 8, 64],
                       [ 12288, 1, 8, 8],
                       [ 2048, 6, 8, 8],
                       [ 2048, 1, 128, 8],
                       [ 2048, 1, 8, 64],
                       [ 12288, 1, 8, 8],
                       [ 2048, 6, 8, 8],
                       [ 2048, 1, 128, 8],
                       [ 2048, 1, 8, 64]])
    
    #normalized_features = whiten(features)
    r, d = kmeans(features1, 4, 1000)
    (code, distor) = vq(features1, r)
    print(code)
    distributions = {}
    for n in code:
        if n in distributions.keys():
            distributions[n] = distributions[n] + 1
        else:
            distributions[n] = 1
    totalCount = len(code)
    totalDistinctCount = len(distributions.keys())
    print(totalCount, totalDistinctCount)
    print(distributions)
    probDis = {}
    for k, v in distributions.items():
        probDis[k] = float(v) / totalCount
    print(calEntropy(probDis))
    
    dis1 = {"a": 0.5, "b": 0.3, "c": 0.2} # less random, smaller entropy
    dis2 = {"a": 0.1, "b": 0.1, "c": 0.1, "d": 0.1, "e": 0.1, "f": 0.1, "g": 0.1, "h": 0.1, "i": 0.1, "j": 0.1} # more random, larger entropy
    print(calEntropy(dis1), calEntropy(dis2))
        