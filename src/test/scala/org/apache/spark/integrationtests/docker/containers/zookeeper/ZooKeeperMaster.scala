package org.apache.spark.integrationtests.docker.containers.zookeeper

import org.apache.curator.framework.CuratorFramework
import org.apache.spark.SparkConf
import org.apache.spark.deploy.master.SparkCuratorUtil
import org.apache.spark.integrationtests.docker.Docker


class ZooKeeperMaster {
  // TODO: build our own ZooKeeper dockerfile
  val container = Docker.launchContainer("jplock/zookeeper")

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


