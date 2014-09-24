package org.apache.spark.integrationtests.docker

import java.io.File
import java.nio.charset.Charset

import com.google.common.io.Files
import org.apache.spark.SparkConf
import org.apache.spark.deploy.master.RecoveryState
import org.json4s.jackson.JsonMethods

import scala.io.Source

abstract class SparkStandaloneBase(conf: SparkConf, sparkEnv: Seq[(String, String)]) {

  private val sparkHome: String = {
    val sparkHome = System.getenv("SPARK_HOME")
    assert(sparkHome != null, "Run with a valid SPARK_HOME")
    sparkHome
  }

  private val confDir = {
    val temp = File.createTempFile("spark-home-temp", "",
      new File(sparkHome, "integration-tests/target/"))
    temp.delete()
    temp.mkdir()
    temp.deleteOnExit()
    temp
  }

  // Save the SparkConf as spark-defaults.conf
  Files.write(conf.getAll.map{ case (k, v) => k + ' ' + v }.mkString("\n"),
    new File(confDir, "spark-defaults.conf"), Charset.forName("UTF-8"))

  // Setup the exports in spark-env.sh
  Files.write(sparkEnv.map{ case (k, v) => s"""export $k="$v""""}.mkString("\n"),
    new File(confDir, "spark-env.sh"), Charset.forName("UTF-8"))

  protected val mountDirs = Seq(
    sparkHome -> "/opt/spark",
    confDir.getAbsolutePath -> "/opt/sparkconf")

  val container: DockerContainer

  def waitForUI(timeoutMillis: Int): Unit = {
    val start = System.currentTimeMillis()
    while ((System.currentTimeMillis() - start) < timeoutMillis) {
      try {
        Source.fromURL(s"http://${container.ip}:8080/json")
        return
      } catch {
        case ce: java.net.ConnectException =>
          Thread.sleep(100)
      }
    }
  }

  def kill() {
    container.kill()
  }
}


class SparkMaster(conf: SparkConf,
                  sparkEnv: Seq[(String, String)]) extends SparkStandaloneBase(conf, sparkEnv) {
  private implicit val formats = org.json4s.DefaultFormats

  val container = Docker.launchContainer("spark-test-master", mountDirs = mountDirs)

  var state: RecoveryState.Value = _
  var liveWorkerIPs: Seq[String] = _
  var numLiveApps = -1

  def masterUrl: String = s"spark://${container.ip}:7077"

  def updateState(): Unit = {
    val json =
      JsonMethods.parse(Source.fromURL(s"http://${container.ip}:8080/json").bufferedReader())
    val workers = json \ "workers"
    val liveWorkers = workers.children.filter(w => (w \ "state").extract[String] == "ALIVE")
    // Extract the worker IP from "webuiaddress" (rather than "host") because the host name
    // on containers is a weird hash instead of the actual IP address.
    liveWorkerIPs = liveWorkers.map {
      w => (w \ "webuiaddress").extract[String].stripPrefix("http://").stripSuffix(":8081")
    }

    numLiveApps = (json \ "activeapps").children.size

    val status = json \\ "status"
    val stateString = status.extract[String]
    state = RecoveryState.values.filter(state => state.toString == stateString).head
  }
}


class SparkWorker(conf: SparkConf,
                  sparkEnv: Seq[(String, String)],
                  masterUrl: String) extends SparkStandaloneBase(conf, sparkEnv) {

  val container = Docker.launchContainer("spark-test-worker",
    args = masterUrl, mountDirs = mountDirs)

}