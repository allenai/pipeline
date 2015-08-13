__author__ = 'rodneykinney'

import os, sys, getopt
from ArrowDetection import *

def usage():
    print 'ExtractArrows.py -i <inputDir> -o <outputDir>'
    sys.exit(1)

print sys.argv
if (len(sys.argv) == 1):
    usage()
inputDir = ''
outputDir = ''
try:
    opts, args = getopt.getopt(sys.argv[1:],"hi:o:",["inputDir=","outputDir="])
except getopt.GetoptError:
    usage()
for opt, arg in opts:
    print opt, arg
    if opt == '-h':
        usage()
    elif opt in ("-i", "--inputDir"):
        inputDir = arg
    elif opt in ("-o", "--outputDir"):
        outputDir = arg

for f in os.listdir(inputDir):
    arrows = findArrows()
    print(arrows)

