import mincemeat
import sys
import math

#use case where input file is not given.
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
    number = int(value)
    binNumber = 0
    while(True):
        if number > 333.3333*(binNumber+1):
            binNumber+= 1
        else:
            break
    yield binNumber, 1


def reducefn(key, values):
    
    bin = {"binNumber":key,"binCount": sum(values)}
    return sum(values)

s = mincemeat.Server()
s.datasource = data
s.mapfn = mapfn
s.reducefn = reducefn
results = s.run_server(password="changeme")
totalNum = len(data)

print len(results)
for bucketNum,val in results.iteritems():
    percentage = (float(val) / float(totalNum)) * 100
    percentage = int(math.ceil(percentage))
    sys.stdout.write(str(bucketNum*333.33))
    sys.stdout.write(' -> ')
    sys.stdout.write(str(round((bucketNum+1)*333.33)))
    sys.stdout.write('   ')
    sys.stdout.write(str(percentage) + " % ")
    if percentage != 100:
        for j in range(percentage):
            sys.stdout.write('*')
    else:
        sys.stdout.write("**********")
    sys.stdout.write("\n")


