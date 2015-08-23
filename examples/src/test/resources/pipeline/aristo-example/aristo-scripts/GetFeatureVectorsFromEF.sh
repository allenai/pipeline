#!/bin/bash

newURL=$(echo "$2" | sed -e 's!/#/evaluation-sets/view/.*/evaluation/\(.*\)!/api/evaluations/\1/feature-vectors!g')

eval "curl $newURL > $1"

