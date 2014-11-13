package org.apache.spark.integrationtests

import java.io.File
import java.util.concurrent.Semaphore

import org.apache.spark.scheduler.{SparkListenerTaskStart, SparkListener}
import org.apache.spark.{SparkException, SparkConf, SparkContext, Logging}
import org.apache.spark.integrationtests.docker.containers.mesos.{MesosSlave, MesosMaster}
import org.apache.spark.integrationtests.docker.containers.zookeeper.ZooKeeperMaster
import org.apache.spark.integrationtests.fixtures.{ZooKeeperFixture, SparkContextFixture, DockerFixture}
import org.scalatest.concurrent.Eventually._
import org.scalatest.{BeforeAndAfterEach, Matchers, FunSuite}
import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps
import org.apache.spark.SparkContext._

class MesosSuite extends FunSuite
  with Matchers
  with Logging
  with BeforeAndAfterEach
  with DockerFixture
  with ZooKeeperFixture
  with SparkContextFixture {

  val MESOS_NATIVE_LIBRARY = sys.env.getOrElse("MESOS_NATIVE_LIBRARY",
    throw new Exception("Need to set MESOS_NATIVE_LIBRARY before running this suite"))
  val SPARK_HOME = sys.env.getOrElse("SPARK_HOME", throw new Exception("SPARK_HOME should be set"))
  val SPARK_INTEGRATION_TESTS_HOME = sys.env.getOrElse("SPARK_INTEGRATION_TESTS_HOME",
    throw new Exception("SPARK_INTEGRATION_TESTS_HOME"))
  val SPARK_DIST = {
    val files = new File(SPARK_HOME).listFiles()
    val dists = files.filter(_.getName.matches("""spark-.*-bin-.*\.tgz"""))
    assert(dists.size !== 0, "Mesos tests require Spark binary distribution .tgz")
    assert(dists.size === 1, "Should only have one Spark binary distribution .tgz")
    dists.head
  }
  val TEST_JAR = {
    val files = new File(SPARK_INTEGRATION_TESTS_HOME, "target/scala-2.10/").listFiles()
    val testJars = files.filter(_.getName.matches("""spark-integration-tests_.*-tests.jar"""))
    assert(testJars.size !== 0, "run test:package before running these tests")
    assert(testJars.size === 1, "Should only have one test jar")
    testJars.head
  }

  var conf: SparkConf = _
  var mesosMaster: MesosMaster = _
  var mesosSlaves: mutable.Seq[MesosSlave] = _

  override def beforeEach() {
    zookeeper = new ZooKeeperMaster()
    mesosMaster = new MesosMaster(zookeeper, sharedDir = Some(new File(SPARK_HOME)))
    mesosSlaves = mutable.Seq(new MesosSlave(zookeeper, sharedDir = Some(new File(SPARK_HOME))))
    conf = new SparkConf()
      .setMaster(mesosMaster.url)
      .setAppName("MesosSuite")
      .set("spark.executor.uri", "/opt/shared-files/" + SPARK_DIST.getName)
      .set("spark.executor.memory", "256m")
      .setJars(Seq(TEST_JAR.getAbsolutePath))
  }

  test("framework registration") {
    eventually(timeout(10 seconds)) {
      mesosMaster.getState.activatedSlaves should be (mesosSlaves.length)
    }
    sc = new SparkContext(conf)
    eventually(timeout(10 seconds)) {
      mesosMaster.getState.numFrameworks should be (1)
    }
  }

  test("basic count job") {
    sc = new SparkContext(conf)
    sc.parallelize(1 to 1000).count() should be (1000)
  }

  test("task cancellation") {
    // Regression test for SPARK-3597.  This code is based on Spark's JobCancellationSuite.
    sc = new SparkContext(conf)
    // Add a listener to release the semaphore once any tasks are launched.
    val sem = new Semaphore(0)
    sc.addSparkListener(new SparkListener {
      override def onTaskStart(taskStart: SparkListenerTaskStart) {
        sem.release()
      }
    })

    val f = sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.countAsync()
    future {
      // Wait until some tasks were launched before we cancel the job.
      sem.acquire()
      f.cancel()
    }
    val e = intercept[SparkException] { Await.result(f, 60 seconds) }
    assert(e.getMessage.contains("cancelled") || e.getMessage.contains("killed"))
  }

}