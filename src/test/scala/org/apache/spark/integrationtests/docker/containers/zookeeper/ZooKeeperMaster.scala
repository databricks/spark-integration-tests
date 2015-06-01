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

package org.apache.spark.integrationtests.docker.containers.zookeeper

import org.apache.curator.framework.CuratorFramework
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkCuratorUtil
import org.apache.spark.integrationtests.docker.Docker


class ZooKeeperMaster {
  // TODO: build our own ZooKeeper dockerfile
  val container = Docker.launchContainer("jplock/zookeeper:3.4.6")

  val zookeeperUrl = s"${container.ip}:2181"

  def newCuratorFramework(): CuratorFramework = {
    val conf = new SparkConf()
    conf.set("spark.deploy.zookeeper.url", zookeeperUrl)
    SparkCuratorUtil.newClient(conf)
  }

  def kill() {
    container.kill()
  }
}
