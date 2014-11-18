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
