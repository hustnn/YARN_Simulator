'''
Created on Mar 14, 2015

@author: niuzhaojie
'''
from numpy import array
from scipy.cluster.vq import vq, kmeans, whiten

if __name__ == '__main__':
    features  = array([[ 8192, 1, 8, 8],
                       [ 1024, 12, 8, 8],
                       [ 1024, 1, 128, 8],
                       [ 1024, 1, 8, 128],
                       [ 8192, 1, 8, 8],
                       [ 1024, 12, 8, 8],
                       [ 1024, 1, 128, 8],
                       [ 1024, 1, 8, 128],
                       [ 8192, 1, 8, 8],
                       [ 1024, 12, 8, 8],
                       [ 1024, 1, 128, 8],
                       [ 1024, 1, 8, 128]])
    r, d = kmeans(features, 4)
    (code, distor) = vq(features, r)
    print(code)