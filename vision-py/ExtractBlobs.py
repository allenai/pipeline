__author__ = 'rodneykinney'

import os, sys, getopt
from BlobDetection import *

def usage():
    print 'ExtractBlobs.py -i <inputDir> -o <outputDir>'
    sys.exit(1)

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

for f in os.listdir(inputDir):
    blobs = findBlobs()
    print(blobs)

