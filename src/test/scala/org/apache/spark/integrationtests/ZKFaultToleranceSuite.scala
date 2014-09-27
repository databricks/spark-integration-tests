package org.apache.spark.integrationtests


import org.apache.spark.deploy.master.RecoveryState
import org.apache.spark.integrationtests.docker.containers.spark.ZooKeeperHASparkStandaloneCluster
import org.apache.spark.integrationtests.docker.{ZooKeeperMaster, Docker}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.scalatest.{Failed, Matchers, FunSuite}
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.Timeouts._

import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * This suite tests the fault-tolerance of the Spark standalone cluster manager and scheduler.
 */
class ZKFaultToleranceSuite extends FunSuite with Matchers with Logging {

  var cluster: ZooKeeperHASparkStandaloneCluster = _
  var zookeeper: ZooKeeperMaster = _
  var sc: SparkContext = _

  val conf: SparkConf = new SparkConf()
  conf.set("spark.executor.memory", "512m")

  override def withFixture(test: NoArgTest) = {
    zookeeper = new ZooKeeperMaster()
    cluster = new ZooKeeperHASparkStandaloneCluster(zookeeper)
    println(s"STARTING TEST ${test.name}")
    try {
      super.withFixture(test) match {
        case failed: Failed =>
          println(s"TEST FAILED: ${test.name}; printing cluster logs")
          cluster.printLogs()
          failed
        case other => other
      }
    } finally {
      if (sc != null) {
        sc.stop()
        sc = null
      }
      cluster.killAll()
      Docker.killAllLaunchedContainers()
    }
  }

  /**
   * Asserts that the cluster is usable and that the expected masters and workers
   * are all alive in a proper configuration (e.g., only one leader).
   */
  def assertValidClusterState(cluster: ZooKeeperHASparkStandaloneCluster) = {
    logInfo(">>>>> ASSERT VALID CLUSTER STATE <<<<<")

    // Check that the cluster is usable (tests client retry logic, so this may take a long
    // time if the cluster is recovering)
    failAfter(120 seconds) {
      val res = sc.parallelize(0 to 10).collect()
      res.toList should be (0 to 10)
    }

    // Check that the cluster eventually reaches a valid state:
    eventually (timeout(120 seconds), interval(1 seconds)) {
      cluster.updateState()
      logDebug("Checking for valid cluster state")
      // There should only be one leader
      val (leaders, nonLeaders) = cluster.masters.partition(_.state == RecoveryState.ALIVE)
      leaders.size should be (1)
      // Any master that is not the leader should be in STANDBY mode:
      nonLeaders.map(_.state).toSet should (be (Set()) or be (Set(RecoveryState.STANDBY)))
      // The workers should be alive and registered with the leader:
      cluster.workers.map(_.container.ip).toSet should be (cluster.getLeader().liveWorkerIPs.toSet)
      // At least one application / driver should be alive
      cluster.getLeader().numLiveApps should be >= 1
    }
  }

  def delay(secs: Duration = 5.seconds) = Thread.sleep(secs.toMillis)

  test("sanity-basic") {
    cluster.addMasters(1)
    cluster.addWorkers(1)
    sc = cluster.createSparkContext()
    assertValidClusterState(cluster)
  }

  test("sanity-many-masters") {
    cluster.addMasters(3)
    cluster.addWorkers(3)
    sc = cluster.createSparkContext()
    assertValidClusterState(cluster)
  }

  test("single-master-halt") {
    cluster.addMasters(3)
    cluster.addWorkers(2)
    sc = cluster.createSparkContext()
    assertValidClusterState(cluster)

    cluster.killLeader()
    delay(30 seconds)
    assertValidClusterState(cluster)
    sc.stop()
    sc = cluster.createSparkContext()
    assertValidClusterState(cluster)
  }

  test("single-master-restart") {
    cluster.addMasters(1)
    cluster.addWorkers(2)
    sc = cluster.createSparkContext()
    assertValidClusterState(cluster)

    cluster.killLeader()
    cluster.addMasters(1)
    delay(30 seconds)
    assertValidClusterState(cluster)

    cluster.killLeader()
    cluster.addMasters(1)
    delay(30 seconds)
    assertValidClusterState(cluster)
  }

  test("cluster-failure") {
    cluster.addMasters(2)
    cluster.addWorkers(2)
    sc = cluster.createSparkContext()
    assertValidClusterState(cluster)

    cluster.workers.foreach(_.kill())
    cluster.masters.foreach(_.kill())
    cluster.masters.clear()
    cluster.workers.clear()
    cluster.addMasters(2)
    cluster.addWorkers(2)
    assertValidClusterState(cluster)
  }

  test("all-but-standby-failure") {
    cluster.addMasters(2)
    cluster.addWorkers(2)
    sc = cluster.createSparkContext()
    assertValidClusterState(cluster)

    cluster.killLeader()
    cluster.workers.foreach(_.kill())
    cluster.workers.clear()
    delay(30 seconds)
    cluster.addWorkers(2)
    assertValidClusterState(cluster)
  }

  test("rolling-outage") {
    cluster.addMasters(1)
    delay()
    cluster.addMasters(1)
    delay()
    cluster.addMasters(1)
    cluster.addWorkers(2)
    sc = cluster.createSparkContext()
    assertValidClusterState(cluster)
    assert(cluster.getLeader() === cluster.masters.head)

    (1 to 3).foreach { _ =>
      cluster.killLeader()
      delay(30 seconds)
      assertValidClusterState(cluster)
      assert(cluster.getLeader() === cluster.masters.head)
      cluster.addMasters(1)
    }
  }
}