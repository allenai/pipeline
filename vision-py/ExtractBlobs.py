__author__ = 'rodneykinney'

import os, json
from VisionCommon import *
from BlobDetection import *

def usage():
    print 'ExtractBlobs.py -i <inputDir> -o <outputDir>'
    sys.exit(1)

inputDir, outputDir = parseArgs(usage)

writeDirectory(inputDir, outputDir, lambda: json.dumps(findBlobs()))

