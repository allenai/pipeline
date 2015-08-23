__author__ = 'rodneykinney'

import os, json, sys, getopt, shutil
from VisionCommon import writeDirectory, joinRelations
from ArrowDetection import *

def usage():
    print 'ExtractRelations.py -a <arrowDir> -t <textDir> -b <blobDir> -o <outputDir>'
    sys.exit(1)

def parseArgs(usage):
    if (len(sys.argv) == 1):
        usage()
    inputDir = ''
    arrowDir = ''
    textDir = ''
    blobDir = ''
    outputDir = ''
    try:
        opts, args = getopt.getopt(sys.argv[1:],"ha:t:b:o:i:",["inputDir=","outputDir="])
    except getopt.GetoptError:
        usage()
    for opt, arg in opts:
        if opt == '-h':
            usage()
        elif opt in ("-i", "--input"):
            inputDir = arg
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

    return inputDir, outputDir

inputDir, outputDir = parseArgs(usage)

for inFile in os.listdir(inputDir):
    outFile = os.path.join(outputDir, inFile)
    shutil.copyfile(os.path.join(inputDir, inFile), outFile)

writeDirectory(inputDir, outputDir, lambda: json.dumps(joinRelations()))


