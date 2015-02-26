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

package org.apache.spark.integrationtests.utils.spark

import java.io.File
import java.nio.charset.Charset

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
    ProcessUtils.runAndCaptureOutput(submitProcessBuilder)
  }

  private def saveSparkConfToTempFile(conf: SparkConf): File = {
    // Save the SparkConf as spark-defaults.conf
    val confFile = File.createTempFile("spark-defaults", ".conf")
    Files.write(conf.getAll.map { case (k, v) => k + ' ' + v}.mkString("\n"), confFile,
      Charset.forName("UTF-8"))
    confFile
  }
}
