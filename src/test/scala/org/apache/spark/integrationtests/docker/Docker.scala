package org.apache.spark.integrationtests.docker

import org.apache.spark.Logging

import scala.collection.mutable
import scala.language.postfixOps
import scala.sys.process._

object Docker extends Logging {
  private val runningDockerContainers = new mutable.HashSet[DockerId]()

  def registerContainer(containerId: DockerId) = this.synchronized {
    runningDockerContainers += containerId
  }

  def killAllLaunchedContainers() = this.synchronized {
    runningDockerContainers.foreach(kill)
  }

  def launchContainer(imageTag: String,
                      args: String = "",
                      mountDirs: Seq[(String, String)] = Seq.empty): DockerContainer = {
    val mountCmd = mountDirs.map{ case (s, t) => s"-v $s:$t" }.mkString(" ")

    val id =
      new DockerId("docker run --privileged -d %s %s %s".format(mountCmd, imageTag, args).!!.trim)
    registerContainer(id)
    try {
      new DockerContainer(id)
    } catch {
      case t: Throwable =>
        kill(id)
        throw t
    }
  }

  def kill(dockerId: DockerId) = this.synchronized {
    "docker kill %s".format(dockerId.id).!
    runningDockerContainers -= dockerId
  }

  def getLogs(dockerId: DockerId): String = {
    s"docker logs ${dockerId.id}".!!
  }

  def dockerHostIp: String = "172.17.42.1" // default docker host ip
}

class DockerId(val id: String) extends AnyVal {
  override def toString: String = id
}
