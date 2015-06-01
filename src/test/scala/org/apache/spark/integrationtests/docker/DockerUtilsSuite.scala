/*
 * Copyright 2015 Databricks Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.integrationtests.docker

import org.apache.spark.SparkConf
import org.apache.spark.deploy.master.{PublicRecoveryState => RecoveryState}
import org.apache.spark.integrationtests.docker.containers.kafka.KafkaBroker
import org.apache.spark.integrationtests.docker.containers.mesos.{MesosSlave, MesosMaster}
import org.apache.spark.integrationtests.docker.containers.spark.{SparkMaster, SparkWorker}
import org.apache.spark.integrationtests.docker.containers.zookeeper.ZooKeeperMaster
import org.apache.spark.integrationtests.fixtures.DockerFixture
import org.apache.spark.integrationtests.utils.kafka.{KafkaProducer, KafkaClient}
import org.scalatest.concurrent.Eventually._
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._
import scala.language.postfixOps

import resource._

class DockerUtilsSuite extends FunSuite with DockerFixture with Matchers {

  test("basic container launching") {
    val container = Docker.launchContainer("ubuntu:precise", args="sleep 10")
    eventually(timeout(10 seconds)) {
      assert(container.ip !== "")
    }
    container.kill()
  }

  test("basic spark cluster") {
    val conf = new SparkConf()
    // Start a master
    val master = new SparkMaster(Seq.empty)
    master.waitForUI(10 seconds)
    val masterState = master.getState
    assert(masterState.numLiveApps === 0)
    assert(masterState.state === RecoveryState.ALIVE)
    assert(masterState.liveWorkerIPs.isEmpty)

    // Add a worker
    val worker = new SparkWorker(Seq.empty, master.masterUrl)
    worker.waitForUI(10 seconds)
    eventually(timeout(10 seconds)) {
      master.getState.liveWorkerIPs should be (Seq(worker.container.ip))
    }

    worker.kill()
    master.kill()
  }

  test("basic zookeeper") {
    val zk = new ZooKeeperMaster()
    for (client <- managed(zk.newCuratorFramework())) {
      assert(client.getZookeeperClient.blockUntilConnectedOrTimedOut())
    }
  }

  test("basic kafka") {
    val zk = new ZooKeeperMaster()
    val broker = new KafkaBroker(zk, brokerId = 0, port = 12345)
    for (
      kafka <- managed(new KafkaClient(zk.zookeeperUrl));
      producer <- managed(new KafkaProducer(broker.brokerAddr))
    ) {
      // Wait for the Kafka brokers to register with ZooKeeper:
      eventually(timeout(10 seconds)) {
        kafka.brokers should be (Seq(0))
      }
      kafka.topics should be (Seq.empty)
      kafka.createTopic("test-topic", numPartitions = 1, replicationFactor = 1)
      kafka.topics should be (Seq("test-topic"))
      kafka.replicasForPartition("test-topic", 0) should be (Seq(0))
      eventually(timeout(10 seconds)) {
        kafka.leaderForPartition("test-topic", 0) should be(Some(0))
      }
      // Try sending messages to the broker:
      producer.send((1 to 10000).map(m => ("test-topic", m.toString)))
    }
  }

  test("basic mesos") {
    val zk = new ZooKeeperMaster()
    val mesosMaster = new MesosMaster(zk)
    val mesosSlave = new MesosSlave(zk)
    eventually(timeout(10 seconds)) {
      mesosMaster.getState.activatedSlaves should be (1)
    }
  }
}
