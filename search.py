#!/usr/bin/env python
import mincemeat
import array
import sys
import hashlib
import collections

def mapfn(key, value):
    value = value.lower()
    import re 
    pattern = re.compile(r"\b([A-Za-z])\b")
    # pattern = re.compile(r"\b([\w\-]+'?[\w\-]*)\b")


    import hashlib
    #Get our hash functions


    for word in value.split():
        match = pattern.match(word.lower())
        if match:
            word = match.group(0)

        hash1 = hashlib.new('md5')
        hash2 = hashlib.new('sha256')
        hash3 = hashlib.new('sha512')
        hash1.update(str(word))
        hash2.update(str(word))
        hash3.update(str(word))
        hashVal1 = hash1.hexdigest()
        hashVal2 = hash2.hexdigest()
        hashVal3 = hash3.hexdigest()

        hashVal1 = int(hashVal1,16) % 200000
        hashVal2 = int(hashVal2,16) % 200000
        hashVal3 = int(hashVal3,16) % 200000
        
        yield "bloom", hashVal1
        yield "bloom", hashVal2
        yield "bloom", hashVal3

def reducefn(key, values):
    bitArray = [0 for x in range(200000)]
    for bit in values:
        bitArray[bit] = 1
    return bitArray




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


#Setup the MapReduce server
s = mincemeat.Server()
s.mapfn = mapfn
s.reducefn = reducefn
s.datasource = data

#Let the user know whats happening
print "starting execution."

#Get the results of the MapReduce
result = s.run_server(password="changeme")
bloomFilter = result["bloom"]
print "text file parsed."
#Allow the user to search the filter
while True:
    wordIn = raw_input("Enter search word: ")
    if(wordIn == 'exit'):
        break
    else:
        import hashlib  
        #Get our hash functions
        hash1 = hashlib.new('md5')
        hash2 = hashlib.new('sha256')
        hash3 = hashlib.new('sha512')
        #Get a copy of the hash functions to use
        #Hash on the word
        hashOut1 = hash1.update(str(wordIn))
        hashOut2 = hash2.update(str(wordIn))
        hashOut3 = hash3.update(str(wordIn))
        #Digest, mmm tasty
        hashOut1 = hash1.hexdigest()
        hashOut2 = hash2.hexdigest()
        hashOut3 = hash3.hexdigest()
        #Convert the hash output to a int % arr size
        wordOut1 = int(hashOut1, 16) % 200000
        wordOut2 = int(hashOut2, 16) % 200000
        wordOut3 = int(hashOut3, 16) % 200000
        #Check to see if the indicies are turned on or off

        if(bloomFilter[wordOut1] == 1 and bloomFilter[wordOut2] == 1 and bloomFilter[wordOut3] == 1):
            print " - Found word!"
        else:
            print " - Word not found."