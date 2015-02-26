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
