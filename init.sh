#!/usr/bin/env bash

# Copyright 2015 Databricks Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
#limitations under the License.

# Setup script which performs environment setup and sanity checks.
# Progress / warning / error messages are printed to stderr; shell exports are
# printed to stdout so that you can run `./init.sh` to perform the sanity
# checks, environment setup, and shell exports.
# This script is designed to be idempotent.

FWDIR="$(cd "`dirname "$0"`"; pwd)"

# -- Helper functions ---------------------------------------------------------

# Like echo, but prints to stderr:
echogreen() { echo -e "\033[32m$@\033[0m" 1>&2; }
echoyellow() { echo -e "\033[33m$@\033[0m" 1>&2; }
echowhite() { echo "$@" 1>&2; }
echored() { echo -e "\033[31m$@\033[0m" 1>&2; }
# Check that an executable exists:
executablexists() { command -v "$@" >/dev/null 2>&1;}

# -- Sanity checks ------------------------------------------------------------

# This has only been tested with OSX + boot2docker, so log a warning if we
# detect that we're running in a different environment:
if [[ `uname` != 'Darwin' ]]; then
   echoyellow "WARNING: this framework has only been tested on OSX."
fi

executablexists boot2docker || echoyellow "WARNING: boot2docker not found; this framework has only been tested with boot2docker!"

if [ ! -x $(executablexists docker) ]; then
    echored "Could not find docker!"
    exit 1
fi

if [ -z "$SPARK_HOME" ]; then
    echored "ERROR: SPARK_HOME must point to a Spark source checkout."
    exit 1
else
    echogreen "Using SPARK_HOME: $SPARK_HOME"
    binary_dist_count=$(ls -1 "$SPARK_HOME" | grep "spark-.*-bin-.*\.tgz" | wc -l)
    if [[ $binary_dist_count -eq 0 ]]; then
        echoyellow "WARNING: Could not find .tgz Spark binary distribution in SPARK_HOME; will not be able to run Mesos tests!"
        echoyellow 'You can build this distribution by running `./make-distribution.sh --tgz` in the SPARK_HOME directory.'
    elif [[ $binary_dist_count -gt 1 ]]; then
        echored "ERROR: Found multiple binary distributions in SPARK_HOME; will not be able to run Mesos tests!"
    else
        echogreen "Found Spark binary distribution"
    fi
    examples_jar_count=$(ls -1 "$SPARK_HOME/examples/target/scala-2.10/" | grep "spark-examples_2.10.*\.jar" | wc -l)
    if [[ $examples_jar_count -eq 0 ]]; then
        echoyellow "WARNING: Could not find Spark examples JAR; will not be able to run SparkStandaloneSuite tests!"
        echoyellow 'You can build this jar by running `sbt/sbt examples/package` in the SPARK_HOME directory'
    elif [[ $examples_jar_count -gt 1 ]]; then
        echored "ERROR: Found multiple binary Spark examples jars in SPARK_HOME; will not be able to run SparkStandaloneSuite tests!"
    else
        echogreen "Found Spark examples JAR"
    fi
    assembly_jar_count=$(ls -1 "$SPARK_HOME/assembly/target/scala-2.10/" | grep "spark-assembly.*hadoop.*\.jar" | wc -l)
    if [[ $assembly_jar_count -eq 0 ]]; then
        echoyellow "WARNING: Could not find Spark assembly JAR; will not be able to run tests that use spark-submit!"
        echoyellow 'You can build this jar by running `sbt/sbt assembly` in the SPARK_HOME directory'
    elif [[ $assembly_jar_count -gt 1 ]]; then
        echored "ERROR: Found multiple binary assemblies in SPARK_HOME; will not be able to run tests that use spark-submit!"
    else
        echogreen "Found Spark assembly JAR"
    fi
fi


# -- Environment setup --------------------------------------------------------

if [ -z "$MESOS_NATIVE_LIBRARY" ]; then
    if [[ `uname` == 'Darwin' ]]; then
        MESOS_NATIVE_LIBRARY="$(brew --repository)/lib/libmesos.dylib"
        if [ ! -f "$MESOS_NATIVE_LIBRARY" ]; then
            unset MESOS_NATIVE_LIBRARY
            echoyellow 'Could not find MESOS_NATIVE_LIBRARY; you can install Mesos using homebrew: `brew install mesos`'
        else
            echogreen "Using MESOS_NATIVE_LIBRARY from Homebrew: $MESOS_NATIVE_LIBRARY"
        fi
    else
        echoyellow "MESOS_NATIVE_LIBRARY is not set; will not be able to run Mesos tests"
    fi
else
    echogreen "MESOS_NATIVE_LIBRARY: $MESOS_NATIVE_LIBRARY"
fi


if [[ `boot2docker status` != 'running' ]]; then
    echoyellow "WARNING: boot2docker is not running!"
fi


# -- Shell init ---------------------------------------------------------------
echogreen "Initialization complete!  Run the following commands in your shell to setup environment variables:"
echo
echo export SPARK_INTEGRATION_TESTS_HOME="$FWDIR"
if [ ! -z "$MESOS_NATIVE_LIBRARY" ]; then
    echo export MESOS_NATIVE_LIBRARY="$MESOS_NATIVE_LIBRARY"
fi
echogreen '# If using boot2docker (this only has to be done after machine / VM reboots):'
echo 'sudo route -n add 172.17.0.0/16 `boot2docker ip`'
