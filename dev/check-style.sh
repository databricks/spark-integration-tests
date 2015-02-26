#!/usr/bin/env bash

FWDIR="$(cd "`dirname $0`"/..; pwd)"
cd "$FWDIR"

sbt scalastyle test:scalastyle
