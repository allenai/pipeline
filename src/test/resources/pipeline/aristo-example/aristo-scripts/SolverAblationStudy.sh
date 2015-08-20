#!/bin/bash

myCommand="cd /Users/tafjord/gitroot/intellij/ari-core; sbt '; project controller ; runMain org.allenai.ari.controller.selector.SolverAblationStudy $@'"

eval $myCommand

