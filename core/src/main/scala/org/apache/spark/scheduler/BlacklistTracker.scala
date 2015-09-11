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

import java.util.concurrent.TimeUnit

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.Success
import org.apache.spark.TaskEndReason
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.SystemClock
import org.apache.spark.util.ThreadUtils
import org.apache.spark.util.Utils


/**
 * BlacklistTracker is design to track problematic executors and node on application level.
 * It is shared by all TaskSet, so that once a new TaskSet coming, it could be benefit from
 * previous experience of other TaskSet.
 *
 * Once task finished, the callback method in TaskSetManager should update failureExecutors.
 */
class BlacklistTracker(sparkConf: SparkConf) {
  // maintain a ExecutorId --> FailureStatus HashMap
  private val failureExecutors: mutable.HashMap[String, FailureStatus] = mutable.HashMap()

  // Apply Strategy pattern here to change different blacklist detection logic
  private val strategy = BlacklistStrategy(sparkConf)

  // A daemon thread to expire blacklist executor periodically
  private val executor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "spark-scheduler-blacklist-expire-timer")

  private val clock = new SystemClock()

  def start(): Unit = {
    val scheduleTask = new Runnable() {
      override def run(): Unit = {
        Utils.logUncaughtExceptions(expireTimeoutFailureExecutors(failureExecutors))
      }
    }
    executor.scheduleAtFixedRate(scheduleTask, 0L, 60, TimeUnit.SECONDS)
  }

  def stop(): Unit = {
    executor.shutdown()
    executor.awaitTermination(10, TimeUnit.SECONDS)
  }

  def updateFailureExecutors(info: TaskInfo, reason: TaskEndReason) : Unit = synchronized {
    reason match {
      case Success => Unit
      case _ =>
        val executorId = info.executorId
        val failureTimes = failureExecutors.get(executorId).fold(0)(_.failureTimes) + 1
        val failedTaskIds = failureExecutors.get(executorId)
          .fold(Set.empty[Long])(_.failedTaskIds) ++ Set(info.taskId)
        val failureStatus = FailureStatus(info.host, failureTimes,
            clock.getTimeMillis(), failedTaskIds)
        failureExecutors.update(executorId, failureStatus)
    }
  }

  def removeFailureExecutors(executorIds: Iterable[String]) : Unit = synchronized {
    executorIds.foreach ( failureExecutors.remove(_))
  }

  def executorIsBlacklisted(executorId: String, sched: TaskSchedulerImpl) : Boolean = {
      executorBlacklist(sched).contains(executorId)
  }

  // The actual implementation is delegated to strategy
  def executorBlacklist(sched: TaskSchedulerImpl): Set[String] = synchronized {
    // If the node is in blacklist, all executors allocated on that node will
    // also be put into  executor blacklist.
    // By default it's turned off, user can enable it in sparkConf.
    val speculationFailedExecutor: Set[String] =
      if (sparkConf.getBoolean("spark.scheduler.blacklist.speculate", false)) {
      Set.empty[String]
    } else {
      nodeBlacklist.flatMap(sched.getExecutorsAliveOnHost(_)
          .getOrElse(Set.empty[String])).toSet
    }

    speculationFailedExecutor ++ strategy.getExecutorBlacklist(failureExecutors)
  }

  // The actual implementation is delegated to strategy
  def nodeBlacklist: Set[String] = synchronized {
    strategy.getNodeBlacklist(failureExecutors)
  }

  // The actual implementation is delegated to strategy
  private def expireTimeoutFailureExecutors(
      failureExecutors: mutable.HashMap[String, FailureStatus]): Unit = synchronized {
    strategy.recoverFailureExecutors(failureExecutors)
  }
}

final case class FailureStatus(
    host: String,
    failureTimes: Int,
    updatedTime: Long,
    failedTaskIds: Set[Long])
