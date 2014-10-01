package org.apache.spark.streaming.integrationtests

import org.apache.spark.integrationtests.docker.containers.kafka.KafkaBroker
import org.apache.spark.integrationtests.docker.containers.spark.{SparkClusters, SparkStandaloneCluster}
import org.apache.spark.integrationtests.docker.containers.zookeeper.ZooKeeperMaster
import org.apache.spark.integrationtests.fixtures.{DockerFixture, SparkClusterFixture, SparkContextFixture, StreamingContextFixture}
import org.apache.spark.integrationtests.utils.kafka.{KafkaProducer, KafkaClient}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf}
import org.scalatest.concurrent.Eventually._
import org.scalatest.{FunSuite, Matchers}
import resource._

import scala.concurrent.duration._
import scala.language.postfixOps

class StreamingFaultToleranceSuite extends FunSuite
  with Matchers
  with Logging
  with DockerFixture
  with SparkClusterFixture[SparkStandaloneCluster]
  with SparkContextFixture
  with StreamingContextFixture {

  test("basic kafka streaming job") {
    cluster = SparkClusters.createStandaloneCluster(Seq.empty, numWorkers = 2)
    val zookeeper = new ZooKeeperMaster()
    val kafkaBroker = new KafkaBroker(zookeeper, brokerId = 1, port = 12345)
    for (
      kafka <- managed(new KafkaClient(zookeeper.zookeeperUrl));
      producer <- managed(new KafkaProducer(kafkaBroker.brokerAddr))
    ) {
      // Wait for the Kafka brokers to register with ZooKeeper:
      eventually(timeout(10 seconds)) {
        kafka.brokers should be (Seq(1))
      }
      val topicName = "defaultTopic"
      kafka.createTopic(topicName, numPartitions = 1, replicationFactor = 1)
      sc = cluster.createSparkContext(new SparkConf())
      val batchDuration = Seconds(1)
      ssc = new StreamingContext(sc, batchDuration)
      val kafkaStream = KafkaUtils.createStream(ssc, zookeeper.zookeeperUrl,
        "streamingIntegrationTests", Map(topicName -> 1))
      kafkaStream.print()
      ssc.start()
      producer.send((1 to 10000).map(m => (topicName, m.toString)))
    }

    /*
    sc = cluster.createSparkContext(new SparkConf())
    val batchDuration = Seconds(1)
    ssc = new StreamingContext(sc, batchDuration)
    val kafkaStream = KafkaUtils.createStream(ssc, zookeeper.zookeeperUrl,
      "streamingIntegrationTests", Map(topicName -> 1))
    val outputStream = new TestOutputStream[(String, String)](kafkaStream)
    outputStream.register()
    def emitBatch() {
      clock.addToTime(batchDuration.milliseconds)
      Thread.sleep(batchDuration.milliseconds)
    }
    ssc.start()
    println(s"The output is ${outputStream.output}")
    val producerConfig = {
      val props = new Properties()
      props.put("metadata.broker.list", kafkaBroker.brokerAddr)
      props.put("serializer.class", classOf[StringEncoder].getName)
      new ProducerConfig(props)
    }
    val producer = new Producer[String, String](producerConfig)
    producer.send(createTestMessage(topicName, Map("a" -> 5, "b" -> 3, "c" -> 10)))
    emitBatch()
    emitBatch()
    println(s"The output is ${outputStream.output}")
    ssc.stop()
    */
  }
}
