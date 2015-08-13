__author__ = 'rodneykinney'

import os, json, sys, getopt
from VisionCommon import writeDirectory, joinRelations
from ArrowDetection import *

def usage():
    print 'ExtractRelations.py -a <arrowDir> -t <textDir> -b <blobDir> -o <outputDir>'
    sys.exit(1)

def parseArgs(usage):
    if (len(sys.argv) == 1):
        usage()
    arrowDir = ''
    textDir = ''
    blobDir = ''
    outputDir = ''
    try:
        opts, args = getopt.getopt(sys.argv[1:],"ha:t:b:o:",["inputDir=","outputDir="])
    except getopt.GetoptError:
        usage()
    for opt, arg in opts:
        if opt == '-h':
            usage()
        elif opt in ("-a", "--arrow"):
            arrowDir = arg
        elif opt in ("-t", "--text"):
            textDir = arg
        elif opt in ("-b", "--blob"):
            blobDir = arg
        elif opt in ("-o", "--output"):
            outputDir = arg

    try:
        os.makedirs(outputDir)
    except:
        ''

    return arrowDir, outputDir

inputDir, outputDir = parseArgs(usage)

writeDirectory(inputDir, outputDir, lambda: json.dumps(joinRelations()))


