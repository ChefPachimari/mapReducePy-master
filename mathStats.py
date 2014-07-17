import mincemeat
import sys
import math

#use case where input file is not given.
data = {}
if len(sys.argv) == 1:
    print "inputFile being set to small"
    inputFile = open("data/small.txt")
    data = dict(enumerate(inputFile))
elif len(sys.argv) > 1:
    try:
        inputFile = open(sys.argv[1])
        data = dict(enumerate(inputFile))
    except:
        print "there was an error with the input file"
        


def mapfn(key, value):
        yield "number", float(value)


def reducefn(key, values):
    print("declaring variables")
    result = {'sum':0,'count':0,'STD':0}

    print("calculating sum")
    for i in range(len(values)):
        result['sum']+= values[i]

    print("calculating the count")
    result['count'] = len(values)

    ###############
    # population standard deviation
    print("determining standard deviation")
    mean = result['sum'] / result['count']
    Diff = []
    for i in range(len(values)):
        stdDevTemp = values[i] - mean
        Diff.append(stdDevTemp)
    totalDiff = 0
    for j in range(len(Diff)):
        import math
        totalDiff += math.pow(Diff[j],2)
    result['STD'] = math.sqrt(totalDiff / len(Diff))
    ###############
    return result

s = mincemeat.Server()
s.datasource = data
s.mapfn = mapfn
s.reducefn = reducefn

print("performing calcuations")
results = s.run_server(password="changeme")

print("calculations complete.  printing results")
mathValues = results["number"]
for key,values in mathValues.iteritems():
    print str(key) + ": " + str(values)
print("mathStats.py Completed execution...\n")
