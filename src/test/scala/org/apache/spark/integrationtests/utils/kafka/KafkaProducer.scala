package org.apache.spark.integrationtests.utils.kafka

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.StringEncoder

class KafkaProducer(brokerAddr: String) extends AutoCloseable {
  private val producerConfig = {
    val props = new Properties()
    props.put("metadata.broker.list", brokerAddr)
    props.put("serializer.class", classOf[StringEncoder].getName)
    // Workaround for https://issues.apache.org/jira/browse/KAFKA-899:
    props.put("retry.backoff.ms", "1000")
    props.put("message.send.max.retries", "10")
    props.put("topic.metadata.refresh.interval.ms", "0")
    props.put("client.id", "SparkIntegrationTests-KafkaProducer")
    new ProducerConfig(props)
  }
  private val producer = new Producer[String, String](producerConfig)

  /**
   * Send a sequence of messages to the Kafka broker.
   * @param messages a sequence of (topic, message) pairs.
   */
  def send(messages: Seq[(String, String)]): Unit = {
    val keyedMessages = messages.map {
      case (topic, msg) => new KeyedMessage[String, String](topic, msg)
    }
    producer.send(keyedMessages: _*)
  }

  override def close() {
    producer.close()
  }
}
