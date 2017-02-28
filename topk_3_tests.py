import numpy as numpy
import time
k = 28
GUI_matrix = ([1,5,2,5,2],[3,8,6,0],[0,7,3],[1,0],[4])
beginning = time.time()
for index,sub in enumerate(GUI_matrix):
    size = len(GUI_matrix[0])-1
    if index > 0:
        while len(sub) <= size:
            sub.insert(0,0)
print GUI_matrix
GUI_matrix = numpy.transpose(GUI_matrix)
print GUI_matrix
counter = 0
while counter <= k:
    for index,sub in enumerate(GUI_matrix):
        for item in sub:
            counter = counter + item
    print counter
print time.time() - beginning
print "at the site %i, the final counter is : %s" % (index+1,counter)
print GUI_matrix

#faire une boucle dans l'index global de 0 a site puis refaire algo du topk normal