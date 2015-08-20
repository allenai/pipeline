__author__ = 'rodneykinney'

import sys, getopt, os
from random import *

def parseArgs(usage):
    if (len(sys.argv) == 1):
        usage()
    inputDir = ''
    outputDir = ''
    try:
        opts, args = getopt.getopt(sys.argv[1:],"hi:o:",["inputDir=","outputDir="])
    except getopt.GetoptError:
        usage()
    for opt, arg in opts:
        if opt == '-h':
            usage()
        elif opt in ("-i", "--inputDir"):
            inputDir = arg
        elif opt in ("-o", "--outputDir"):
            outputDir = arg

    try:
        os.makedirs(outputDir)
    except:
        raise

    return inputDir, outputDir

def writeDirectory(inputDir, outputDir, content):
    for inFile in os.listdir(inputDir):
        outFile = os.path.join(outputDir, inFile+".json")
        w = open(outFile,'w')
        w.write(content())
        w.close()

def joinRelations():
    pos = (uniform(0,100), uniform(0,100))
    size = (uniform(0,10), uniform(0,10))
    return {'headtopleft': {'X':pos[0], 'Y':pos[1]},
            'headbottomright': {'X':pos[0]+size[0], 'Y':pos[1]+size[1]}}






