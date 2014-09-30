package org.apache.spark.integrationtests.utils.kafka

import kafka.admin.CreateTopicCommand
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.Logging

/**
 * Wrapper for interacting with Kafka.
 */
class KafkaClient(zookeeperUrl: String) extends AutoCloseable with Logging {
  private val zkClient = new ZkClient(zookeeperUrl)

  def createTopic(topic: String, numPartitions: Int, replicationFactor: Int) {
    CreateTopicCommand.createTopic(zkClient, topic, numPartitions, replicationFactor)
  }

  def topics: Seq[String] = {
    ZkUtils.getAllTopics(zkClient)
  }

  def brokers: Seq[Int] = {
    ZkUtils.getChildren(zkClient, ZkUtils.BrokerIdsPath).map(_.toInt).sorted
  }

  override def close() {
    zkClient.close()
  }
}
