package org.apache.spark.integrationtests

import java.io.File

import org.apache.spark.{SparkConf, SparkContext, Logging}
import org.apache.spark.integrationtests.docker.containers.mesos.{MesosSlave, MesosMaster}
import org.apache.spark.integrationtests.docker.containers.zookeeper.ZooKeeperMaster
import org.apache.spark.integrationtests.fixtures.{SparkContextFixture, DockerFixture}
import org.scalatest.concurrent.Eventually._
import org.scalatest.{Matchers, FunSuite}
import scala.concurrent.duration._
import scala.language.postfixOps


class MesosSuite extends FunSuite
  with Matchers
  with Logging
  with DockerFixture
  with SparkContextFixture {

  val MESOS_NATIVE_LIBRARY = sys.env.getOrElse("MESOS_NATIVE_LIBRARY",
    throw new Exception("Need to set MESOS_NATIVE_LIBRARY before running this suite"))
  val SPARK_HOME = sys.env.getOrElse("SPARK_HOME", throw new Exception("SPARK_HOME should be set"))
  val SPARK_DIST = {
    val files = new File(SPARK_HOME).listFiles()
    val dists = files.filter(_.getName.matches("""spark-.*-bin-.*\.tgz"""))
    assert(dists.size === 1, "Should only have one Spark binary distribution JAR")
    dists.head
  }

  test("running basic jobs") {
    val zk = new ZooKeeperMaster()
    val mesosMaster = new MesosMaster(zk, sharedDir = Some(new File(SPARK_HOME)))
    val mesosSlave = new MesosSlave(zk, sharedDir = Some(new File(SPARK_HOME)))
    println(mesosSlave.container.id)
    eventually(timeout(10 seconds)) {
      mesosMaster.getState.activatedSlaves should be (1)
    }
    val conf = new SparkConf()
      .setMaster(mesosMaster.url)
      .setAppName("mesos task killing test")
      .set("spark.executor.uri", "/opt/shared-files/" + SPARK_DIST.getName)
    sc = new SparkContext(conf)
    eventually(timeout(10 seconds)) {
      mesosMaster.getState.numFrameworks should be (1)
    }
    // Test that the Spark cluster can run a basic job:
    sc.parallelize(1 to 1000).count() should be (1000)
  }

}