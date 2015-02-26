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
