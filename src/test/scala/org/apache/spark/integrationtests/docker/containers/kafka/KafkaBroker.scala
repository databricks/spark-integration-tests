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

package org.apache.spark.integrationtests.docker.containers.kafka

import org.apache.spark.integrationtests.docker.Docker
import org.apache.spark.integrationtests.docker.containers.zookeeper.ZooKeeperMaster

class KafkaBroker(zookeeper: ZooKeeperMaster, val brokerId: Int, val port: Int) {
  require(zookeeper.container.isRunning,
    "ZooKeeper container should be running before launching Kafka container")

  val container = {
    val args: String = Seq(
      s"--link ${zookeeper.container.name}:zk",
      s"-e BROKER_ID=$brokerId",
      s"-e PORT=$port"
    ).mkString(" ")
    Docker.launchContainer("spark-kafka-0.8", dockerArgs=args)
  }

  val brokerAddr: String = container.ip + ":" + port
}
