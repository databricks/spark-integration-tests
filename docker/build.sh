#!/usr/bin/env bash

docker build -t spark-test-base base
docker build -t spark-test-master spark-master
docker build -t spark-test-worker spark-worker
