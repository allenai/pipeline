__author__ = 'rodneykinney'

import os, json
from VisionCommon import *
from TextDetection import *

def usage():
    print 'ExtractText.py -i <inputDir> -o <outputDir>'
    sys.exit(1)

inputDir, outputDir = parseArgs(usage)

writeDirectory(inputDir, outputDir, lambda: json.dumps(findText()))

