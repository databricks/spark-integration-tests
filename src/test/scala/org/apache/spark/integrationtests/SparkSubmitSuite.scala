package org.apache.spark.integrationtests

import java.io.File

import org.apache.spark.integrationtests.docker.containers.spark.{SparkClusters, SparkStandaloneCluster}
import org.apache.spark.integrationtests.fixtures.{DockerFixture, SparkClusterFixture, SparkContextFixture}
import org.apache.spark.integrationtests.utils.spark.SparkSubmitUtils
import org.apache.spark.{Logging, SparkConf}
import org.scalatest.concurrent.Eventually._
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._
import scala.language.postfixOps

class SparkSubmitSuite extends FunSuite
  with Matchers
  with Logging
  with DockerFixture
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

  /**
   * Test spark-submit's `cluster` deploy mode by running the SparkPi example.
   *
   * @param conf the SparkConf to use
   * @param jarURL the URL of the JAR in the shared cluster filesystem
   */
  private def testSparkSubmitClusterModeWithSparkPi(conf: SparkConf, jarURL: String) {
    cluster = SparkClusters.createStandaloneCluster(Seq.empty, numWorkers = 1)
    val submitOptions = Seq(
      "--deploy-mode", "cluster",
      "--class", "org.apache.spark.examples.SparkPi",
      "--master", cluster.getMasterUrl())
    val (exitCode, stdout, stderr) =
      SparkSubmitUtils.submitAndCaptureOutput(conf, submitOptions, jarURL, Seq("1"))
    if (exitCode != 0) {
      fail(s"spark-submit exited with code $exitCode\nstdout:$stdout\nstderr$stderr")
    }
    val driverId = {
      val driverIdRegex = """driver-\d+-\d+""".r
      driverIdRegex findFirstIn stdout match {
        case Some(id) => id
        case None => fail(s"Couldn't parse driver id from spark submit output:\n$stdout")
      }
    }
    logInfo(s"Launched driver with id $driverId")
    assert(!stdout.contains("FAILED"))
    cluster.masters.head.getState.numLiveApps should (be(0) or be(1))
    eventually(timeout(60 seconds), interval(1 seconds)) {
      cluster.masters.head.getState.numLiveApps should be(0)
      cluster.masters.head.getState.numCompletedApps should be(1)
    }
  }

  test("spark-submit cluster mode without JAR URL scheme") {
    // Regression test for SPARK-4434
    val conf = new SparkConf().set("spark.executor.memory", "256m")
    val examplesJar = new File("/opt/spark/", EXAMPLES_JAR.stripPrefix(SPARK_HOME)).getAbsolutePath
    testSparkSubmitClusterModeWithSparkPi(conf, examplesJar)
  }

  test("spark-submit cluster mode with file:// JAR URL scheme") {
    // Regression test for SPARK-4434
    val conf = new SparkConf().set("spark.executor.memory", "256m")
    val examplesJar =
      "file://" + new File("/opt/spark/", EXAMPLES_JAR.stripPrefix(SPARK_HOME)).getAbsolutePath
    testSparkSubmitClusterModeWithSparkPi(conf, examplesJar)
  }

  test("spark-submit with cluster mode with spark.driver.host set on submitter's machine") {
    // Regression test for SPARK-4253
    val conf = new SparkConf()
    conf.set("spark.executor.memory", "256m")
    conf.set("spark.driver.host", "SOME-NONEXISTENT-HOST")
    val examplesJar = new File("/opt/spark/", EXAMPLES_JAR.stripPrefix(SPARK_HOME)).getAbsolutePath
    testSparkSubmitClusterModeWithSparkPi(conf, examplesJar)
  }
}