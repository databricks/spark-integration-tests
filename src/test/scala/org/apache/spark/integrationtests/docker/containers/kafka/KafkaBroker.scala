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
    Docker.launchContainer("wurstmeister/kafka:0.8.1", dockerArgs=args)
  }

  val brokerAddr: String = container.ip + ":" + port
}
