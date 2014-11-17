package org.apache.spark.integrationtests

import org.apache.spark.integrationtests.docker.containers.spark.{SparkClusters, SparkStandaloneCluster}
import org.apache.spark.integrationtests.fixtures.{NetworkFaultInjectorFixture, DockerFixture, SparkClusterFixture, SparkContextFixture}
import org.apache.spark.Logging
import org.scalatest.concurrent.Eventually._
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._
import scala.language.postfixOps

class SparkStandaloneSuite extends FunSuite
  with Matchers
  with Logging
  with DockerFixture
  with NetworkFaultInjectorFixture
  with SparkClusterFixture[SparkStandaloneCluster]
  with SparkContextFixture {

  val SPARK_HOME = sys.env.getOrElse("SPARK_HOME", throw new Exception("SPARK_HOME should be set"))

  test("workers should reconnect to master if disconnected due to transient network issues") {
    // Regression test for SPARK-3736
    val env = Seq(
      "SPARK_MASTER_OPTS" -> "-Dspark.worker.timeout=2",
      "SPARK_WORKER_OPTS" -> "-Dspark.worker.timeout=1 -Dspark.akka.timeout=1 -Dspark.akka.failure-detector.threshold=1 -Dspark.akka.heartbeat.interval=1"
    )
    cluster = SparkClusters.createStandaloneCluster(env, numWorkers = 1)
    val master = cluster.masters.head
    val worker = cluster.workers.head
    eventually(timeout(30 seconds), interval(1 seconds)) {
      master.getState.liveWorkerIPs.size should be (1)
    }
    logInfo("Cluster launched with one worker")

    networkFaultInjector.dropTraffic(master.container, worker.container)
    networkFaultInjector.dropTraffic(worker.container, master.container)
    eventually(timeout(30 seconds), interval(1 seconds)) {
      master.getState.liveWorkerIPs.size should be (0)
    }
    Thread.sleep(10000)
    logInfo("Master shows that zero workers are registered after network connection fails")

    networkFaultInjector.restore()
    eventually(timeout(30 seconds), interval(1 seconds)) {
      master.getState.liveWorkerIPs.size should be (1)
    }
    logInfo("Master shows one worker after network connection is restored")
  }
}