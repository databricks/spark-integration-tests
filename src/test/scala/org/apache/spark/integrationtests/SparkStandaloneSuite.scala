package org.apache.spark.integrationtests

import java.io.File
import java.nio.charset.Charset

import com.google.common.io.Files
import org.apache.spark.integrationtests.docker.containers.spark.{SparkClusters, SparkStandaloneCluster}
import org.apache.spark.integrationtests.fixtures.{NetworkFaultInjectorFixture, DockerFixture, SparkClusterFixture, SparkContextFixture}
import org.apache.spark.{Logging, SparkConf}
import org.scalatest.concurrent.Eventually._
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.sys.process.Process


class SparkStandaloneSuite extends FunSuite
  with Matchers
  with Logging
  with DockerFixture
  with NetworkFaultInjectorFixture
  with SparkClusterFixture[SparkStandaloneCluster]
  with SparkContextFixture {

  val SPARK_HOME = sys.env.getOrElse("SPARK_HOME", throw new Exception("SPARK_HOME should be set"))
  val EXAMPLES_JAR = {
    val examplesTargetDir = new File(SPARK_HOME, "examples/target/scala-2.10/")
    val jars = examplesTargetDir.listFiles().filter(_.getName.endsWith(".jar"))
      .filter(_.getName.startsWith("spark-examples_2.10"))
    assert(jars.size === 1, "Should only have one Spark Examples JAR")
    jars.head.getAbsolutePath
  }

  def saveSparkConf(conf: SparkConf, file: File) {
    // Save the SparkConf as spark-defaults.conf
    Files.write(conf.getAll.map { case (k, v) => k + ' ' + v}.mkString("\n"), file,
      Charset.forName("UTF-8"))
  }

  test("spark-submit with cluster mode with spark.driver.host set on submitter's machine") {
    cluster = SparkClusters.createStandaloneCluster(Seq.empty, numWorkers = 1)
    val confFile = File.createTempFile("spark-defaults", ".conf")
    val conf = new SparkConf()
    conf.set("spark.executor.memory", "256m")
    conf.set("spark.driver.host", "SOME-NONEXISTENT-HOST")
    saveSparkConf(conf, confFile)
    val submitCommand =
      Array(new File(SPARK_HOME, "bin/spark-submit").getAbsolutePath,
          "--deploy-mode", "cluster",
          "--class", "org.apache.spark.examples.SparkPi",
          "--master", cluster.getMasterUrl(),
          "--properties-file", confFile.getAbsolutePath,
          new File("/opt/spark/", EXAMPLES_JAR.stripPrefix(SPARK_HOME)).getAbsolutePath,
          "1")
    val submitProcessBuilder = new ProcessBuilder(submitCommand: _*)
    // We need to clear the environment variables in order to work around SPARK-3734;
    // Let's keep this workaround in place even after SPARK-3734 is fixed in order to
    // more easily run regression-tests against older Spark versions:
    submitProcessBuilder.environment().clear()
    val sparkSubmitOutput = Process(submitProcessBuilder).!!
    val driverId = {
      val driverIdRegex = """driver-\d+-\d+""".r
      driverIdRegex findFirstIn sparkSubmitOutput match {
        case Some(id) => id
        case None => fail(s"Couldn't parse driver id from spark submit output:\n$sparkSubmitOutput")
      }
    }
    println(s"Launched driver with id $driverId")
    assert(!sparkSubmitOutput.contains("FAILED"))
    cluster.masters.head.getState.numLiveApps should (be (0) or be(1))
    eventually(timeout(60 seconds), interval(1 seconds)) {
      cluster.masters.head.getState.numLiveApps should be (0)
      cluster.masters.head.getState.numCompletedApps should be (1)
    }
  }

  test("workers should reconnect to master if disconnected due to transient network issues") {
    // Regression test for SPARK-3736
    val env = Seq(
      "SPARK_MASTER_OPTS" -> "-Dspark.worker.timeout=2",
      "SPARK_WORKER_OPTS" -> "-Dspark.worker.timeout=1 -Dspark.akka.timeout=1 -Dspark.akka.failure-detector.threshold=1 -Dspark.akka.heartbeat.interval=1"
    )
    cluster = SparkClusters.createStandaloneCluster(env, numWorkers = 1)
    val master = cluster.masters.head
    val worker = cluster.workers.head
    master.getState.liveWorkerIPs.size should be (1)
    println("Cluster launched with one worker")

    networkFaultInjector.dropTraffic(master.container, worker.container)
    networkFaultInjector.dropTraffic(worker.container, master.container)
    eventually(timeout(30 seconds), interval(1 seconds)) {
      master.getState.liveWorkerIPs.size should be (0)
    }
    Thread.sleep(10000)
    println("Master shows that zero workers are registered after network connection fails")

    networkFaultInjector.restore()
    eventually(timeout(30 seconds), interval(1 seconds)) {
      master.getState.liveWorkerIPs.size should be (1)
    }
    println("Master shows one worker after network connection is restored")
  }
}