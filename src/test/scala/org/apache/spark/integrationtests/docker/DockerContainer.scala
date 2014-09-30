package org.apache.spark.integrationtests.docker

import java.util.concurrent.TimeoutException

import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._

import scala.concurrent.duration.Duration
import scala.language.postfixOps
import scala.sys.process._

class DockerContainer(val id: DockerId) {

  private implicit val formats = DefaultFormats

  private val inspectJson: JValue = parse(s"docker inspect ${id.id}".!!)

  val ip: String = (inspectJson \ "NetworkSettings" \ "IPAddress").extractOpt[String].get
  val name: String = (inspectJson \ "Name").extractOpt[String].get

  def blockUntilRunning(timeout: Duration) {
    val start = System.currentTimeMillis()
    while ((System.currentTimeMillis() - start) < timeout.toMillis) {
        if (isRunning) {
          return
        }
        Thread.sleep(100)
    }
    throw new TimeoutException(
      s"Timed out after $timeout waiting for container $name to be running")
  }

  def isRunning = {
    (parse(s"docker inspect ${id.id}".!!) \ "State" \ "Running").extractOpt[Boolean].get
  }

  def getLogs(): String = {
    Docker.getLogs(this.id)
  }

  def kill() {
    Docker.kill(this.id)
  }
}
