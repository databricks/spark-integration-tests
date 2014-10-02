package org.apache.spark.integrationtests.docker.containers.mesos

import java.io.File

import org.apache.spark.integrationtests.docker.Docker
import org.apache.spark.integrationtests.docker.containers.zookeeper.ZooKeeperMaster
import org.json4s.JsonAST.JArray
import org.json4s.jackson.JsonMethods

import scala.io.Source


class MesosMaster(zookeeper: ZooKeeperMaster, sharedDir: Option[File] = None) {
  val container = {
    val dockerArgs = Seq(
      // First 3 args are required; see http://mesos.apache.org/documentation/latest/configuration/
      s"-e MESOS_WORK_DIR=/tmp/mesos/work",
      s"-e MESOS_QUORUM=1",
      s"-e MESOS_ZK=zk://${zookeeper.zookeeperUrl}/mesos",
      s"-e MESOS_LOG_DIR=/var/log/mesos"  // without this, the default is to write no log files
    ).mkString(" ")
    val mountDirs = sharedDir.map(workDir => Seq(workDir.getAbsolutePath -> "/opt/shared-files"))
      .getOrElse(Seq.empty)
    Docker.launchContainer("redjack/mesos-master", dockerArgs = dockerArgs, mountDirs = mountDirs)
  }

  val port = 5050
  val url = s"mesos://${container.ip}:$port"

  case class MesosMasterState(activatedSlaves: Int, numFrameworks: Int)

  def getState: MesosMasterState = {
    implicit val formats = org.json4s.DefaultFormats
    val json =
      JsonMethods.parse(Source.fromURL(s"http://${container.ip}:$port/state.json").bufferedReader())
    val activatedSlaves = (json \ "activated_slaves").extract[Int]
    val numFrameworks = (json \ "frameworks").asInstanceOf[JArray].values.size

    MesosMasterState(activatedSlaves, numFrameworks)
  }

  def kill() {
    container.kill()
  }
}

/**
 * Launch a Mesos slave in a Docker container.
 *
 * @param zookeeper  A ZooKeeper container
 * @param sharedDir  A host directory that will be mounted in the container under /opt/shared-files
 */
class MesosSlave(zookeeper: ZooKeeperMaster, sharedDir: Option[File] = None) {
  val container = {
    val dockerArgs = Seq(
      s"-e MESOS_MASTER=zk://${zookeeper.zookeeperUrl}/mesos",
      s"-e MESOS_LOG_DIR=/var/log/mesos",  // without this, the default is to write no log files
      s"-e MESOS_SWITCH_USER=0" // because the host machine's users aren't present in Docker
    ).mkString(" ")
    val mountDirs = sharedDir.map(workDir => Seq(workDir.getAbsolutePath -> "/opt/shared-files"))
      .getOrElse(Seq.empty)
    Docker.launchContainer("redjack/mesos-slave", dockerArgs = dockerArgs, mountDirs = mountDirs)
  }

  val port = 5051

  def kill() {
    container.kill()
  }
}