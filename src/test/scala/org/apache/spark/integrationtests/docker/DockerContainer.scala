package org.apache.spark.integrationtests.docker

import scala.language.postfixOps
import scala.sys.process._

import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._

class DockerContainer(val id: DockerId) {

  private implicit val formats = DefaultFormats

  private val inspectJson: JValue = parse(s"docker inspect ${id.id}".!!)

  val ip: String = (inspectJson \ "NetworkSettings" \ "IPAddress").extractOpt[String].get

  def getLogs(): String = {
    Docker.getLogs(this.id)
  }

  def kill() {
    Docker.kill(this.id)
  }
}
