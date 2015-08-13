__author__ = 'rodneykinney'

import os, json
from VisionCommon import *
from TextDetection import *

def usage():
    print 'ExtractText.py -i <inputDir> -o <outputDir>'
    sys.exit(1)

inputDir, outputDir = parseArgs(usage)

# for inFile in os.listdir(inputDir):
#     outFile = os.path.join(outputDir, inFile)
#     w = open(outFile,'w')
#     w.write(json.dumps(findText()))
#     w.close()

writeDirectory(inputDir, outputDir, lambda: json.dumps(findText()))

