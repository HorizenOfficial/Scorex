#!/bin/bash

set -eo pipefail
retval=0

# Building a package
echo "" && echo "=== Building a package ===" && echo ""

# Compiling and running tests
# sbt -ivy ./.ivy2 -sbt-dir ./.sbt "project sparkz" +clean +coverage +test || retval="$?"

exit "$retval"

