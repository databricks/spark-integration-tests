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

import scala.util.Properties

import sbt._
import sbt.Keys._


object BuildSettings {
  val buildSettings = Defaults.defaultSettings ++ Seq(
    organization := "org.apache.spark",
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.10.4",
    resolvers ++= Seq(
      Resolver.sonatypeRepo("snapshots"),
      Resolver.sonatypeRepo("releases"),
      Resolver.typesafeRepo("releases"),
      "JAnalyse Repository" at "http://www.janalyse.fr/repository/"
    ),
    parallelExecution in Test := false,
    // This fork avoids "Native Library already loaded in another classloader" errors:
    fork in Test := true
  )
}


object SparkIntegrationTestsBuild extends Build {

  import BuildSettings._

  val SPARK_HOME = Properties.envOrNone("SPARK_HOME").getOrElse {
    "/Users/joshrosen/Documents/spark"
    //throw new Exception("SPARK_HOME must be defined")
  }

  lazy val sparkCore = ProjectRef(
    file(SPARK_HOME),
    "core"
  )


  lazy val sparkStreaming = ProjectRef(
    file(SPARK_HOME),
    "streaming"
  )

  lazy val streamingKafka = ProjectRef(
    file(SPARK_HOME),
    "streaming-kafka"
  )

  lazy val root = Project(
    "spark-integration-tests",
    file("."),
    settings = buildSettings ++ Seq(
      scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
      libraryDependencies ++= Seq(
        "com.jsuereth" %% "scala-arm" % "1.4",
        "fr.janalyse"   %% "janalyse-ssh" % "0.9.14",
        "com.jcraft" % "jsch" % "0.1.51",
        "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test",
        "net.sf.jopt-simple" % "jopt-simple" % "3.2" % "test"  // needed by Kafka, excluded by Spark
      )
    )
  ).dependsOn(sparkCore, sparkStreaming, streamingKafka)
}
