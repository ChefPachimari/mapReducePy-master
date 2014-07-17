#!/usr/bin/env python
import mincemeat
import array
import sys
import hashlib
import collections

def mapfn(key, value):
	import hashlib
	print "performing map on at " + str(value[0]) + " and " + str(value[len(value)-1])
	for i in value:
		word = "tim" + str(i)
		hashObj = hashlib.new('md5')
		hashObj.update(str(word))
		hashVal = hashObj.hexdigest()
		if hashVal.startswith("00000"):
		# if "00000" in hashVal:
			yield "key",hashVal


def reducefn(key, values):
	return values

#Setup the MapReduce server
s = mincemeat.Server()
s.mapfn = mapfn
s.reducefn = reducefn
ARRAY_LENGTH = 100000
ARRAY_COUNT = 40000000 / ARRAY_LENGTH

dataBank = []
print "creating DataBank"
for i in range(ARRAY_COUNT):
	tempData = []
	lower = i*ARRAY_LENGTH
	upper = ((i+1) * ARRAY_LENGTH) - 1
	for j in range(lower,upper):
		tempData.append(j)
	dataBank.append(tempData)
print "DataBank made"
s.datasource = dict(enumerate(dataBank))

#Let the user know whats happening
print "starting execution."

#Get the results of the MapReduce
result = s.run_server(password="changeme")
print result