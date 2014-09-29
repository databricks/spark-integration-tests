package org.apache.spark.integrationtests.docker.containers.spark

import java.io.File
import java.nio.charset.Charset

import com.google.common.io.Files
import org.apache.spark.{Logging, SparkContext, SparkConf}
import org.apache.spark.deploy.master.RecoveryState
import org.apache.spark.integrationtests.docker.{ZooKeeperMaster, Docker, DockerContainer}
import org.json4s.jackson.JsonMethods

import scala.collection.mutable.ListBuffer
import scala.io.Source

abstract class SparkStandaloneBase(sparkEnv: Seq[(String, String)]) {

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


class SparkMaster(sparkEnv: Seq[(String, String)]) extends SparkStandaloneBase(sparkEnv) {

  val container = Docker.launchContainer("spark-test-master", mountDirs = mountDirs)

  def masterUrl: String = s"spark://${container.ip}:7077"

  case class SparkMasterState(state: RecoveryState.Value,
                              liveWorkerIPs: Seq[String],
                              numLiveApps: Int)

  def getUpdatedState: SparkMasterState = {
    implicit val formats = org.json4s.DefaultFormats
    val json =
      JsonMethods.parse(Source.fromURL(s"http://${container.ip}:8080/json").bufferedReader())

    val state = {
      val status = json \\ "status"
      val stateString = status.extract[String]
      RecoveryState.values.filter(state => state.toString == stateString).head
    }

    val liveWorkerIPs = {
      val workers = json \ "workers"
      val liveWorkers = workers.children.filter(w => (w \ "state").extract[String] == "ALIVE")
      // Extract the worker IP from "webuiaddress" (rather than "host") because the host name
      // on containers is a weird hash instead of the actual IP address.
      liveWorkers.map {
        w => (w \ "webuiaddress").extract[String].stripPrefix("http://").stripSuffix(":8081")
      }
    }

    val numLiveApps = (json \ "activeapps").children.size

    SparkMasterState(state, liveWorkerIPs, numLiveApps)
  }
}


class SparkWorker(sparkEnv: Seq[(String, String)],
                  masterUrl: String) extends SparkStandaloneBase(sparkEnv) {

  val container = Docker.launchContainer("spark-test-worker",
    args = masterUrl, mountDirs = mountDirs)

}


class SparkStandaloneCluster(baseEnv: Seq[(String, String)]) extends Logging {
  val masters = ListBuffer[SparkMaster]()
  val workers = ListBuffer[SparkWorker]()

  def getSparkEnv: Seq[(String, String)] = {
    baseEnv
  }

  def addWorkers(num: Int){
    logInfo(s">>>>> ADD WORKERS $num <<<<<")
    val masterUrl = getMasterUrl()
    (1 to num).foreach { _ => workers += new SparkWorker(getSparkEnv, masterUrl) }
    workers.foreach(_.waitForUI(10000))
  }


  def addMasters(num: Int) {
    logInfo(s">>>>> ADD MASTERS $num <<<<<")
    (1 to num).foreach { _ => masters += new SparkMaster(getSparkEnv) }
    masters.foreach(_.waitForUI(10000))
  }

  def createSparkContext(conf: SparkConf, name: String ="spark-integration-tests"): SparkContext = {
    // Counter-hack: Because of a hack in SparkEnv#create() that changes this
    // property, we need to reset it.
    System.setProperty("spark.driver.port", "0")
    new SparkContext(getMasterUrl(), name, conf)
  }

  def getMasterUrl(): String = {
    "spark://" + masters.map(_.masterUrl.stripPrefix("spark://")).mkString(",")
  }

  def printLogs() = {
    def separator() = println((1 to 79).map(_ => "-").mkString)
    masters.foreach { master =>
      separator()
      println(s"Master ${master.container.id} log")
      separator()
      println(master.container.getLogs())
      println()
    }
    workers.foreach { worker =>
      separator()
      println(s"Worker ${worker.container.id} log")
      separator()
      println(worker.container.getLogs())
      println()
    }
  }

  def killAll() {
    masters.foreach(_.kill())
    workers.foreach(_.kill())
  }
}


class ZooKeeperHASparkStandaloneCluster(baseEnv: Seq[(String, String)], zookeeper: ZooKeeperMaster)
  extends SparkStandaloneCluster(baseEnv) {

  override def getSparkEnv = {
    super.getSparkEnv ++ Seq(
      "SPARK_DAEMON_JAVA_OPTS" -> ("-Dspark.deploy.recoveryMode=ZOOKEEPER " +
        s"-Dspark.deploy.zookeeper.url=${zookeeper.zookeeperUrl}")
    )
  }

  def killLeader() {
    logInfo(">>>>> KILL LEADER <<<<<")
    val leader = getLeader()
    masters -= leader
    leader.kill()
  }

  def getLeader(): SparkMaster = {
    val leaders = masters.filter(_.getUpdatedState.state == RecoveryState.ALIVE)
    assert(leaders.size == 1)
    leaders.head
  }
}


object SparkClusters {
  def createStandaloneCluster(baseEnv: Seq[(String, String)],
                              numWorkers: Int): SparkStandaloneCluster = {
    val cluster = new SparkStandaloneCluster(baseEnv)
    cluster.addMasters(1)
    cluster.addWorkers(numWorkers)
    cluster
  }
}