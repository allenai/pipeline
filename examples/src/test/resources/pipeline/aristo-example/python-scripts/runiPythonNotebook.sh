#!/bin/bash

base=$(basename "$1")
base2=$(echo $base | sed -e 's!\.ipynb!!g')

command="env iPython_inputDir=$2 ipython nbconvert --to=html --ExecutePreprocessor.enabled=True $1"

command2="mv $base2.html $3"

eval $command
eval $command2
