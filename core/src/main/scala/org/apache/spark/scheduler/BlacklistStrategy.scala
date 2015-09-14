/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.util.SystemClock

/**
 * The interface to determine executor blacklist and node blacklist.
 */
trait BlacklistStrategy {
  val expireTimeInMillisecond: Long
  
  def getExecutorBlacklist(failedExecutorMap: mutable.HashMap[String, FailureStatus]): Set[String]
  def getNodeBlacklist(failedExecutorMap: mutable.HashMap[String, FailureStatus]): Set[String]

  // Default implementation to remove failure executors from HashMap based on given time period.
  def expireExecutorsInBlackList(failedExecutorMap: mutable.HashMap[String, FailureStatus]): Unit = {
    val now = new SystemClock().getTimeMillis()
    failedExecutorMap.retain((executorid, failureStatus) => {
      (now - failureStatus.updatedTime) < expireTimeInMillisecond
    })
  }
}

/**
 * A type of blacklist strategy:
 *
 * An executor will be in blacklist, if it failed more than "executorFailedThreshold" times.
 * A node will be in blacklist, if there are more than "nodeFailedThreshold" executors on it
 * in executor blacklist
 */
class SimpleStrategy(
    maxFailureTaskNumber: Int,
    maxBlackExecutorNumber: Int,
    val expireTimeInMillisecond: Long
  )extends BlacklistStrategy {

  private def getSelectedExecutorMap(
      failedExecutorMap: mutable.HashMap[String, FailureStatus]) = {
    failedExecutorMap.filter{
      case (id, failureStatus) => failureStatus.failureTimes > maxFailureTaskNumber
    }
  }

  def getExecutorBlacklist(
      failureExecutors: mutable.HashMap[String, FailureStatus]): Set[String] = {
    getSelectedExecutorMap(failureExecutors).keys.toSet
  }

  def getNodeBlacklist(failureExecutors: mutable.HashMap[String, FailureStatus]): Set[String] = {
    getSelectedExecutorMap(failureExecutors)
      .groupBy{case (id, failureStatus) => failureStatus.host}
      .filter {case (host, failureExecutors) => failureExecutors.size > maxBlackExecutorNumber}
      .keys.toSet
  }
}

/**
 * Create BlacklistStrategy instance according to SparkConf
 */
object BlacklistStrategy {
  def apply(sparkConf: SparkConf): BlacklistStrategy = {
    val timeout = sparkConf.getLong("spark.scheduler.blacklist.timeout", 1800000L)
    sparkConf.get("spark.scheduler.blacklist.strategy", "threshold") match {
      case "threshold" =>
        new SimpleStrategy(
            sparkConf.getInt("spark.scheduler.blacklist.threshold.maxFailureTaskNumber", 3),
            sparkConf.getInt("spark.scheduler.blacklist.threshold.maxBlackExecutorNumber", 3),
            timeout)
      case "strict" =>
        // A special case of SimpleStrategy, Once task failed  at executor,
        // put the executor and its node into blacklist.
        new SimpleStrategy(0,0,timeout)
      case unsupported =>
        throw new Exception(s"No match blacklist strategy for $unsupported")
    }
  }
}
