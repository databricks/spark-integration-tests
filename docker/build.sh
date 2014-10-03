#!/usr/bin/env bash

# Build our own custom images:
docker build -t spark-test-base base
docker build -t spark-test-master spark-master
docker build -t spark-test-worker spark-worker
docker build -t spark-kafka-0.8 kafka

# Install images from the Docker central repository:
docker pull redjack/mesos-master
docker pull redjack/mesos-slave
docker pull jplock/zookeeper
