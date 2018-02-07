from collections import Counter

dup1 = []
dup2 = []

with open('./outfile', 'r') as f:
    l1 = f.read().split('\n')


with open('src/test/resources/project3/expected/query14_humanreadable', 'r') as f:
    l2 = f.read().split('\n')


x = list(set(l2).difference(l1))
import numpy as np
print(np.matrix(x))
print(len(x))
