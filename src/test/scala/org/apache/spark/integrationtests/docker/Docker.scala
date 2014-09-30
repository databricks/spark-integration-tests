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

  /**
   * Launch a docker container.
   *
   * @param imageTag    the container image
   * @param args        arguments to pass to the container's default command (e.g. spark master url)
   * @param dockerArgs  arguments to pass to `docker` to control the container configuration
   * @param mountDirs   List of (localDirectory, containerDirectory) pairs for mounting directories
   *                    in container.
   * @return            A `DockerContainer` handle for interacting with the launched container.
   */
  def launchContainer(imageTag: String,
                      args: String = "",
                      dockerArgs: String = "",
                      mountDirs: Seq[(String, String)] = Seq.empty): DockerContainer = {
    val mountCmd = mountDirs.map{ case (s, t) => s"-v $s:$t" }.mkString(" ")

    val dockerLaunchCommand = s"docker run --privileged -d $mountCmd $dockerArgs $imageTag $args"
    logDebug(s"Docker launch command is $dockerLaunchCommand")
    val id = new DockerId(dockerLaunchCommand.!!.trim)
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
