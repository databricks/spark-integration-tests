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

import java.io.{PrintWriter, ByteArrayOutputStream}

import scala.sys.process.{ProcessLogger, Process}

object ProcessUtils {
  /**
   * Run a process defined by the given [[java.lang.ProcessBuilder]] and retrieve its
   * output as a string.
   *
   * @return (exitValue, stdout, stderr)
   */
  def runAndCaptureOutput(processBuilder: ProcessBuilder): (Int, String, String) = {
    val process = Process(processBuilder)
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
}
