package org.apache.spark.integrationtests.fixtures

import org.apache.spark.SparkContext
import org.apache.spark.integrationtests.docker.Docker
import org.apache.spark.integrationtests.docker.containers.spark.SparkStandaloneCluster
import org.apache.spark.integrationtests.docker.containers.zookeeper.ZooKeeperMaster
import org.apache.spark.streaming.StreamingContext
import org.scalatest.{Failed, Suite, SuiteMixin}

// See the "Composing fixtures by stacking traits" section of the ScalaTest manual
// for an explanation of this pattern: http://www.scalatest.org/user_guide/sharing_fixtures
//
// When these traits are stacked in a test suite, the setup is performed top-to-bottom
// while the cleanup is performed bottom-to-top.  For example, if I define a suite
//
//    class MyTestSuite extends FunSuite
//      with DockerFixture
//      with SparkClusterFixture[SparkStandaloneCluster]
//      ...
//
// then DockerFixture's setup will be called before SparkClusterFixture; after the
// test completes, we will perform cleanup in the reverse order, so SparkClusterFixture
// will stop the SparkCluster before DockerFixture kills all remaining containers that were launched
// from this suite.

trait SparkContextFixture extends SuiteMixin { this: Suite =>
  var sc: SparkContext = _
  abstract override def withFixture(test: NoArgTest) = {
    try {
      super.withFixture(test)
    } finally {
      if (sc != null) {
        sc.stop()
        sc = null
      }
    }
  }
}

trait StreamingContextFixture extends SuiteMixin { this: Suite =>
  var ssc: StreamingContext = _
  abstract override def withFixture(test: NoArgTest) = {
    try {
      super.withFixture(test)
    } finally {
      if (ssc != null) {
        ssc.stop()
        ssc = null
      }
    }
  }
}

trait ZooKeeperFixture extends SuiteMixin { this: Suite =>
  var zookeeper: ZooKeeperMaster = _
  abstract override def withFixture(test: NoArgTest) = {
    try {
      super.withFixture(test)
    } finally {
      if (zookeeper != null) {
        zookeeper.kill()
        zookeeper = null
      }
    }
  }
}

trait SparkClusterFixture[T <: SparkStandaloneCluster] extends SuiteMixin { this: Suite =>
  var cluster: T = _
  abstract override def withFixture(test: NoArgTest) = {
    try {
      super.withFixture(test) match {
        case failed: Failed =>
          if (cluster != null) {
            println(s"TEST FAILED: ${test.name}; printing cluster logs")
            cluster.printLogs()
          }
          failed
        case other => other
      }
    } finally {
      if (cluster != null) {
        cluster.killAll()
        cluster = null.asInstanceOf[T]
      }
    }
  }
}

trait DockerFixture extends SuiteMixin { this: Suite =>
  abstract override def withFixture(test: NoArgTest) = {
    try {
      super.withFixture(test)
    } finally {
      Docker.killAllLaunchedContainers()
    }
  }
}