package org.apache.spark.integrationtests.utils.kafka

import kafka.admin.CreateTopicCommand
import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.Logging

/**
 * Wrapper for interacting with Kafka.
 */
class KafkaClient(zookeeperUrl: String) extends AutoCloseable with Logging {
  private val zkClient = {
    val zkConnectionTimeout = 6000
    val zkSessionTimeout = 6000
    new ZkClient(zookeeperUrl, zkConnectionTimeout, zkSessionTimeout, ZKStringSerializer)
  }

  type BrokerId = Int

  def createTopic(topic: String, numPartitions: Int, replicationFactor: Int) {
    CreateTopicCommand.createTopic(zkClient, topic, numPartitions, replicationFactor, "0")
  }

  def topics: Seq[String] = {
    ZkUtils.getAllTopics(zkClient)
  }

  def brokers: Seq[BrokerId] = {
    ZkUtils.getChildren(zkClient, ZkUtils.BrokerIdsPath).map(_.toInt).sorted
  }

  def replicasForPartition(topic: String, partition: Int): Seq[BrokerId] = {
    ZkUtils.getReplicasForPartition(zkClient, topic, partition)
  }

  def leaderForPartition(topic: String, partition: Int): Option[BrokerId] = {
    ZkUtils.getLeaderForPartition(zkClient, topic, partition)
  }

  override def close() {
    zkClient.close()
  }
}
