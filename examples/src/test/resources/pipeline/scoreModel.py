# Sample python file to demonstrate use of external scripts in a pipeline
import sys
f = open(sys.argv[1],'w')
f.write("Placeholder for model scoring output\n")
f.close()

