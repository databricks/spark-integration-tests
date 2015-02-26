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

package org.apache.spark.integrationtests

import java.io.File
import java.nio.charset.Charset

import scala.language.postfixOps

import com.google.common.io.Files
import org.scalatest.{FunSuite, Matchers}

import org.apache.spark.integrationtests.fixtures.SparkContextFixture
import org.apache.spark.Logging
import org.apache.spark.integrationtests.utils.spark.ProcessUtils


/**
 * Tests mechanisms for adding JARs to the PySpark driver's classpath.
 */
class PySparkAddJarsSuite extends FunSuite
  with Matchers
  with Logging
  with SparkContextFixture {

  private val SPARK_HOME =
    sys.env.getOrElse("SPARK_HOME", throw new Exception("SPARK_HOME should be set"))

  private val EXAMPLES_JAR = {
    val examplesTargetDir = new File(SPARK_HOME, "examples/target/scala-2.10/")
    val jars = examplesTargetDir.listFiles().filter(_.getName.endsWith(".jar"))
      .filter(_.getName.startsWith("spark-examples_2.10"))
    assert(jars.size === 1, "Should only have one Spark Examples JAR")
    jars.head.getAbsolutePath
  }

  // TODO: we should probably create our own JAR instead of using the Spark examples one
  // TODO: check that the JARs are also added to the executor classpaths
  private val testScript =
    """
      | from pyspark import SparkContext
      | from py4j.java_gateway import java_import
      | import sys
      |
      | sc = SparkContext(appName="PySparkAddJarsSuite")
      | print >> sys.stderr, "CLASSPATH:", sc._jvm.System.getProperty("java.class.path")
      | java_import(sc._jvm, "org.apache.spark.examples.SparkTC")
      |
      | assert(sc._jvm.SparkTC.numEdges() == 200)
      | print "DONE!"
    """.stripMargin.lines.map(_.drop(1)).mkString("\n")

  private val testScriptFile = {
    val file = File.createTempFile("test", ".py")
    Files.write(testScript, file, Charset.forName("UTF-8"))
    file
  }

  private def testDashDashJars(execName: String) {
    val execPath = new File(SPARK_HOME, s"./bin/$execName").getAbsolutePath
    val pb = new ProcessBuilder(execPath, "--jars", EXAMPLES_JAR, testScriptFile.getAbsolutePath)
    val (exitCode, stdout, stderr) = ProcessUtils.runAndCaptureOutput(pb)
    if (exitCode != 0 || stdout != "DONE!") {
      fail(s"$execName exited with code $exitCode; stdout:\n$stdout\nstderr:\n$stderr")
    }
  }

  test("check that classes from added JAR are not available if JAR is not added") {
    // This test helps us to ensure the validity of this test suite.  If the Java class that we're
    // testing for is already present on the classpath before we've added the JAR, then all of the
    // other tests in this suite would trivially pass.
    val execPath = new File(SPARK_HOME, "./bin/pyspark").getAbsolutePath
    val pb = new ProcessBuilder(execPath, testScriptFile.getAbsolutePath)
    val (exitCode, stdout, stderr) = ProcessUtils.runAndCaptureOutput(pb)
    if (exitCode == 0 || stdout == "DONE!") {
      fail(s"Expected pyspark to fail; stdout:\n$stdout\nstderr:\n$stderr")
    }
  }

  // TODO: test("./bin/pyspark --jars (interactive shell)") { }

  test("./bin/pyspark --jars test.py") {
    testDashDashJars("pyspark")
  }

  test("./bin/spark-submit --jars test.py") {
    testDashDashJars("spark-submit")
  }
}
