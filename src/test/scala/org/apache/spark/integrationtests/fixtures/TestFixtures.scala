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

package org.apache.spark.integrationtests.fixtures

import fr.janalyse.ssh.{SSH, SSHShell}
import org.apache.spark.SparkContext
import org.apache.spark.integrationtests.docker.Docker
import org.apache.spark.integrationtests.docker.containers.spark.SparkStandaloneCluster
import org.apache.spark.integrationtests.docker.containers.zookeeper.ZooKeeperMaster
import org.apache.spark.integrationtests.docker.network.{IPTables, NetworkFaultInjector}
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

trait NetworkFaultInjectorFixture extends SuiteMixin { this: Suite =>
  private var ssh: SSH = _
  private var sshShell: SSHShell = _
  private var ipTables: IPTables = _
  var networkFaultInjector: NetworkFaultInjector = _

  abstract override def withFixture(test: NoArgTest) = {
    try {
      ssh = Docker.getHostSSHConnection
      sshShell = new SSHShell()(ssh)
      ipTables = IPTables(sshShell)
      networkFaultInjector = NetworkFaultInjector(ipTables)
      super.withFixture(test)
    } finally {
      Docker.killAllLaunchedContainers()
      if (ipTables != null) {
        ipTables.deleteAddedChains()
      }
      if (sshShell != null) {
        sshShell.close()
      }
      if (ssh != null) {
        ssh.close()
      }
    }
  }
}
