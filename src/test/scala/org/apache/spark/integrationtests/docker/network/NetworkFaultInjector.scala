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

package org.apache.spark.integrationtests.docker.network

import fr.janalyse.ssh.SSHShell
import org.apache.spark.integrationtests.docker.DockerContainer

import scala.collection.mutable


/**
 * Inject network faults (such as partitions or packet loss) into the connections
 * between docker containers by modifying the `iptables` configuration on the host
 * running the containers.
 *
 * This code is based on https://github.com/dcm-oss/blockade
 */
case class NetworkFaultInjector(ipTables: IPTables) {
  import IPTables._
  import ipTables._

  /** Generate an identifier for naming an `iptables` chain */
  private def failChainName(prefix: String, a: DockerContainer, b: DockerContainer): Chain =
    s"$prefix-${a.id.id.take(8)}-${a.id.id.take(8)}"

  /**
   * Drop all traffic from the source container to the destination container.
   * Traffic from the destination to the source will not be affected.
   */
  def dropTraffic(src: DockerContainer, dest: DockerContainer) {
    val chain: Chain = failChainName("drop", src, dest)
    createChain(chain)
    insertRule(chain, src.ip, dest.ip, "DROP")
    insertRule("FORWARD", src=src.ip, target = chain)
  }

  /**
   * Reject all traffic from the source container to the destination container.
   * Traffic from the destination to the source will not be affected.
   */
  def rejectTraffic(src: DockerContainer, dest: DockerContainer) {
    val chain: Chain = failChainName("reject", src, dest)
    createChain(chain)
    insertRule(chain, src.ip, dest.ip, "REJECT")
    insertRule("FORWARD", src=src.ip, target = chain)
  }

  /**
   * Restore full network connectivity by removing all `iptables` rules created by us.
   */
  def restore() {
    ipTables.deleteAddedChains()
  }
}

object IPTables {
  type IP = String
  type Chain = String
}

/**
 * Class for programatically modifying `iptables` firewall entries via an SSH connection.
 *
 * This code is based on https://github.com/dcm-oss/blockade.
 */
case class IPTables(shell: SSHShell) {
  import IPTables._

  /** Represents an `iptables` rule that is part of a chain */
  case class Rule(
    chain: String,
    target: String,
    prot: String,
    opt: String,
    src: String,
    dest: String)

  /** A list of chains that we've created, to facilitate automated cleanup */
  private val chains = mutable.Set[Chain]()

  private def runCommand(command: String): String = {
    val cmd = s"sudo iptables $command"
    println(s"Running command $cmd")
    val (res, status) = shell.executeWithStatus(cmd)
    assert(status == 0, s"Command $res had non-zero exit status ($status)")
    res
  }

  /**
   * Create a new `iptables` chain with a given name.
   */
  def createChain(name: Chain) {
    runCommand(s"-N $name")
    chains.add(name)
  }

  /**
   * List all of the rules in an `iptables` chain.
   */
  def getRules(chain: Chain): Seq[Rule] = {
    val lines = runCommand(s"-L $chain").lines.toArray.toSeq
    assert(lines(0).contains("Chain"))
    assert(lines(1).startsWith("target"))
    lines.drop(2).map { line =>
      val fields = line.split("\\s+")
      Rule(chain, fields(0), fields(1), fields(2), fields(3), fields(4))
    }
  }

  def insertRule(chain: Chain, src: IP = "", dest: IP = "", target: Chain) {
    require(src != "" || dest != "", "Must specify src, dest, or both")
    var args = Seq("-I", chain)
    if (src != "") args = args ++ Seq("-s", src)
    if (dest != "") args = args ++ Seq("-d", dest)
    args = args ++ Seq("-j", target)
    runCommand(args.mkString(" "))
  }

  def deleteRule(rule: Rule) {
    // Here, we identify the rule to be deleted by using the rule contents rather than
    // its position / id within its chain.  This avoids race-conditions if other processes
    // are concurrently modifying `iptables`
    val args = Seq("-D", rule.chain, "-s", rule.src, "-j", rule.target)
    runCommand(args.mkString(" "))
  }

  /**
   * Delete an `iptables` chain, plus all rules in other chains that refer to it.
   */
  def deleteChain(name: Chain) {
    // In order to delete a chain, we must first delete all rules referencing it
    // (e.g. rules that forward traffic into the chain)
    val rulesReferencingChain = getRules("FORWARD").filter(_.target == name)
    rulesReferencingChain.foreach(deleteRule)
    // Delete the chain itself:
    runCommand(s"-F $name")
    runCommand(s"-X $name")
    chains.remove(name)
  }

  /**
   * Delete all chains added from this `IPTables` instance.
   */
  def deleteAddedChains() {
    chains.iterator.foreach(deleteChain)
  }
}
