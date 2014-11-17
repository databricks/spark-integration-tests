package org.apache.spark.integrationtests.utils.spark

import java.io.{PrintWriter, ByteArrayOutputStream, File}
import java.nio.charset.Charset

import scala.sys.process.{ProcessLogger, Process}
import scala.language.postfixOps

import com.google.common.io.Files

import org.apache.spark.SparkConf


/**
 * Programmatic wrapper around `spark-submit`
 */
object SparkSubmitUtils {

  private val SPARK_HOME =
    sys.env.getOrElse("SPARK_HOME", throw new Exception("SPARK_HOME should be set"))

  /**
   * Submit a job through SparkSubmit and retrieve its output as a string.
   *
   * @param conf a SparkConf used to configure this application; this will be saved to a file
   *             and passed to spark-submit via its `--properties-file` argument
   * @param submitOptions a list of options that will be passed to spark-submit
   * @param appJar the application JAR / Python file to run
   * @param appOptions a list of options that will be passed to the application
   * @return (exitValue, stdout, stderr)
   */
  def submitAndCaptureOutput(
      conf: SparkConf,
      submitOptions: Seq[String],
      appJar: String,
      appOptions: Seq[String] = Seq.empty): (Int, String, String) = {
    val submitExecutable = new File(SPARK_HOME, "bin/spark-submit").getAbsolutePath
    val confFile = saveSparkConfToTempFile(conf).getAbsolutePath
    val submitCommand = Seq(submitExecutable) ++ submitOptions ++
      Seq("--properties-file", confFile) ++ Seq(appJar) ++ appOptions
    val submitProcessBuilder = new ProcessBuilder(submitCommand: _*)
    // We need to clear the environment variables in order to work around SPARK-3734;
    // Let's keep this workaround in place even after SPARK-3734 is fixed in order to
    // more easily run regression-tests against older Spark versions:
    submitProcessBuilder.environment().clear()
    val process = Process(submitProcessBuilder)
    // See http://stackoverflow.com/a/15438493/590203
    val stdout = new ByteArrayOutputStream
    val stderr = new ByteArrayOutputStream
    val stdoutWriter = new PrintWriter(stdout)
    val stderrWriter = new PrintWriter(stderr)
    val exitValue = process.!(ProcessLogger(stdoutWriter.println, stderrWriter.println))
    stdoutWriter.close()
    stderrWriter.close()
    (exitValue, stdout.toString, stderr.toString)

  }

  private def saveSparkConfToTempFile(conf: SparkConf): File = {
    // Save the SparkConf as spark-defaults.conf
    val confFile = File.createTempFile("spark-defaults", ".conf")
    Files.write(conf.getAll.map { case (k, v) => k + ' ' + v}.mkString("\n"), confFile,
      Charset.forName("UTF-8"))
    confFile
  }
}
