#!/usr/bin/env python
import mincemeat
import sys
import math

if len(sys.argv) == 1:
    print "inputFile being set to mobydick.txt"
    inputFile = open("data/mobydick.txt")
    data = dict(enumerate(inputFile))
elif len(sys.argv) > 1:
    try:
        inputFile = open(sys.argv[1])
        data = dict(enumerate(inputFile))
    except:
        print "there was an error with the input file"

# The data source can be any dictionary-like object
datasource = data
#key = Key
#value = Value
def mapfn(key, value):
    for w in value.split():
        yield w.lower(), 1

#key = Key
#ks = ?
def reducefn(key, values):
    result = sum(values)
    return result

s = mincemeat.Server()
s.datasource = datasource
print("begin map function")
s.mapfn = mapfn
print("finished map function")

print("begin reduction")
s.reducefn = reducefn
print("finished map function")

results = s.run_server(password="changeme")
tempList = []

for key, val in results.iteritems():
    keyValList = [val,key]
    tempList.append(keyValList)
tempList.sort(reverse=True)

for i in range(40):
    print "count: " + str(tempList[i][0]) + " Word: " + str(tempList[i][1])

print("wc.py execution complete...\n")
