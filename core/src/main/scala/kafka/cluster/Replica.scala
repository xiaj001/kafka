/**
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

package kafka.cluster

import kafka.server.epoch.LeaderEpochFileCache
import kafka.log.{Log, LogOffsetSnapshot}
import kafka.utils.Logging
import kafka.server.{LogOffsetMetadata, LogReadResult}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.utils.Time

/**
  * 每个分区可以有多个副本(Replica)，并且会从其副本集合中选出一个副本作为Leader副本，
  * 所有的读写请求都有选举出的Leader副本处理。剩余的其它副本都作为Follower副本，Follower副本
  * 会从Leader副本处获取消息并更新到自己的Log中。Follower副本是Leader副本的热备份。
  *
  * 本地副本:副本对应的Log分配在当前的Broker上。
  * 远程副本:副本对应的Log分配在其它的Broker上，在当前Broker上仅维护了副本的LEO等信息。
  *
  * 一个副本是 本地副本还是远程副本 与它是 Leader副本还是Follower副本 没有直接联系。
  */
class Replica(val brokerId: Int,  // 标识该副本所在的Broker的id
              val topicPartition: TopicPartition,
              time: Time = Time.SYSTEM,
              initialHighWatermarkValue: Long = 0L,
              @volatile var log: Option[Log] = None // 本地副本对应的Log对象，远程副本此字段为空。
             ) extends Logging {

  // the high watermark offset value, in non-leader replicas only its message offsets are kept
  // LogOffsetMetadata对象，此字段用来记录 HW(HighWatermark) 的值。消费者只能获取到HW之前的消息，
  // 其后的消息对消费者是不可见的。此字段由Leader副本负责更新维护，更新时机是消息被ISR集合中所有
  // 副本成功同步，即消息被成功提交。
  @volatile private[this] var highWatermarkMetadata = new LogOffsetMetadata(initialHighWatermarkValue)

  // the log end offset value, kept in all replicas;
  // for local replica it is the log's end offset, for remote replicas its value is only updated by follower fetch
  // 对于本地副本，此字段记录的是追加到Log中的最新消息的offset，可以直接从Log.nextOffsetMetadata字段中获取
  // 对于远程副本，此字段含义相同，但是由其他Broker发送请求来更新此值，并不能直接从本地获取到
  @volatile private[this] var logEndOffsetMetadata = LogOffsetMetadata.UnknownOffsetMetadata
  // the log start offset value, kept in all replicas;
  // for local replica it is the log's start offset, for remote replicas its value is only updated by follower fetch
  @volatile private[this] var _logStartOffset = Log.UnknownLogStartOffset

  // The log end offset value at the time the leader received the last FetchRequest from this follower
  // This is used to determine the lastCaughtUpTimeMs of the follower
  @volatile private[this] var lastFetchLeaderLogEndOffset = 0L

  // The time when the leader received the last FetchRequest from this follower
  // This is used to determine the lastCaughtUpTimeMs of the follower
  @volatile private[this] var lastFetchTimeMs = 0L

  // lastCaughtUpTimeMs is the largest time t such that the offset of most recent FetchRequest from this follower >=
  // the LEO of leader at time t. This is used to determine the lag of this follower and ISR of this partition.
  // 用于记录Follower副本最后一次追赶上Leader的时间戳
  @volatile private[this] var _lastCaughtUpTimeMs = 0L

  // 根据log是否为空，从而确定 是否是 本地副本
  def isLocal: Boolean = log.isDefined

  def lastCaughtUpTimeMs: Long = _lastCaughtUpTimeMs

  val epochs: Option[LeaderEpochFileCache] = log.map(_.leaderEpochCache)

  info(s"Replica loaded for partition $topicPartition with initial high watermark $initialHighWatermarkValue")
  log.foreach(_.onHighWatermarkIncremented(initialHighWatermarkValue))

  /*
   * If the FetchRequest reads up to the log end offset of the leader when the current fetch request is received,
   * set `lastCaughtUpTimeMs` to the time when the current fetch request was received.
   *
   * Else if the FetchRequest reads up to the log end offset of the leader when the previous fetch request was received,
   * set `lastCaughtUpTimeMs` to the time when the previous fetch request was received.
   *
   * This is needed to enforce the semantics of ISR, i.e. a replica is in ISR if and only if it lags behind leader's LEO
   * by at most `replicaLagTimeMaxMs`. These semantics allow a follower to be added to the ISR even if the offset of its
   * fetch request is always smaller than the leader's LEO, which can happen if small produce requests are received at
   * high frequency.
   */
  def updateLogReadResult(logReadResult: LogReadResult) {
    // 更新LEO
    if (logReadResult.info.fetchOffsetMetadata.messageOffset >= logReadResult.leaderLogEndOffset)
      _lastCaughtUpTimeMs = math.max(_lastCaughtUpTimeMs, logReadResult.fetchTimeMs)
    else if (logReadResult.info.fetchOffsetMetadata.messageOffset >= lastFetchLeaderLogEndOffset)
      _lastCaughtUpTimeMs = math.max(_lastCaughtUpTimeMs, lastFetchTimeMs)

    logStartOffset = logReadResult.followerLogStartOffset
    logEndOffset = logReadResult.info.fetchOffsetMetadata
    lastFetchLeaderLogEndOffset = logReadResult.leaderLogEndOffset
    lastFetchTimeMs = logReadResult.fetchTimeMs
  }

  def resetLastCaughtUpTime(curLeaderLogEndOffset: Long, curTimeMs: Long, lastCaughtUpTimeMs: Long) {
    lastFetchLeaderLogEndOffset = curLeaderLogEndOffset
    lastFetchTimeMs = curTimeMs
    _lastCaughtUpTimeMs = lastCaughtUpTimeMs
  }

  private def logEndOffset_=(newLogEndOffset: LogOffsetMetadata) {
    if (isLocal) {
      // 对于本地副本，不能直接更新LEO，其LEO由Log.logEndOffsetMetadata字段决定
      throw new KafkaException(s"Should not set log end offset on partition $topicPartition's local replica $brokerId")
    } else {
      // 对于远程副本，LEO是通过请求进行更新的
      logEndOffsetMetadata = newLogEndOffset
      trace(s"Setting log end offset for replica $brokerId for partition $topicPartition to [$logEndOffsetMetadata]")
    }
  }

  def logEndOffset: LogOffsetMetadata =  // 获取LEO，本地副本和远程副本的获取方式不同
    if (isLocal)
      log.get.logEndOffsetMetadata
    else
      logEndOffsetMetadata

  /**
   * Increment the log start offset if the new offset is greater than the previous log start offset. The replica
   * must be local and the new log start offset must be lower than the current high watermark.
   */
  def maybeIncrementLogStartOffset(newLogStartOffset: Long) {
    if (isLocal) {
      if (newLogStartOffset > highWatermark.messageOffset)
        throw new OffsetOutOfRangeException(s"Cannot increment the log start offset to $newLogStartOffset of partition $topicPartition " +
          s"since it is larger than the high watermark ${highWatermark.messageOffset}")
      log.get.maybeIncrementLogStartOffset(newLogStartOffset)
    } else {
      throw new KafkaException(s"Should not try to delete records on partition $topicPartition's non-local replica $brokerId")
    }
  }

  private def logStartOffset_=(newLogStartOffset: Long) {
    if (isLocal) {
      throw new KafkaException(s"Should not set log start offset on partition $topicPartition's local replica $brokerId " +
                               s"without attempting to delete records of the log")
    } else {
      _logStartOffset = newLogStartOffset
      trace(s"Setting log start offset for remote replica $brokerId for partition $topicPartition to [$newLogStartOffset]")
    }
  }

  def logStartOffset: Long =
    if (isLocal)
      log.get.logStartOffset
    else
      _logStartOffset

  def highWatermark_=(newHighWatermark: LogOffsetMetadata) {
    if (isLocal) {  // 只有本地副本可以更新HW
      if (newHighWatermark.messageOffset < 0)
        throw new IllegalArgumentException("High watermark offset should be non-negative")

      highWatermarkMetadata = newHighWatermark
      log.foreach(_.onHighWatermarkIncremented(newHighWatermark.messageOffset))
      trace(s"Setting high watermark for replica $brokerId partition $topicPartition to [$newHighWatermark]")
    } else {
      throw new KafkaException(s"Should not set high watermark on partition $topicPartition's non-local replica $brokerId")
    }
  }

  def highWatermark: LogOffsetMetadata = highWatermarkMetadata

  /**
   * The last stable offset (LSO) is defined as the first offset such that all lower offsets have been "decided."
   * Non-transactional messages are considered decided immediately, but transactional messages are only decided when
   * the corresponding COMMIT or ABORT marker is written. This implies that the last stable offset will be equal
   * to the high watermark if there are no transactional messages in the log. Note also that the LSO cannot advance
   * beyond the high watermark.
   */
  def lastStableOffset: LogOffsetMetadata = {
    log.map { log =>
      log.firstUnstableOffset match {
        case Some(offsetMetadata) if offsetMetadata.messageOffset < highWatermark.messageOffset => offsetMetadata
        case _ => highWatermark
      }
    }.getOrElse(throw new KafkaException(s"Cannot fetch last stable offset on partition $topicPartition's " +
      s"non-local replica $brokerId"))
  }

  /*
   * Convert hw to local offset metadata by reading the log at the hw offset.
   * If the hw offset is out of range, return the first offset of the first log segment as the offset metadata.
   */
  def convertHWToLocalOffsetMetadata() {
    if (isLocal) {
      highWatermarkMetadata = log.get.convertToOffsetMetadata(highWatermarkMetadata.messageOffset).getOrElse {
        log.get.convertToOffsetMetadata(logStartOffset).getOrElse {
          val firstSegmentOffset = log.get.logSegments.head.baseOffset
          new LogOffsetMetadata(firstSegmentOffset, firstSegmentOffset, 0)
        }
      }
    } else {
      throw new KafkaException(s"Should not construct complete high watermark on partition $topicPartition's non-local replica $brokerId")
    }
  }

  def offsetSnapshot: LogOffsetSnapshot = {
    LogOffsetSnapshot(
      logStartOffset = logStartOffset,
      logEndOffset = logEndOffset,
      highWatermark =  highWatermark,
      lastStableOffset = lastStableOffset)
  }

  override def equals(that: Any): Boolean = that match {
    case other: Replica => brokerId == other.brokerId && topicPartition == other.topicPartition
    case _ => false
  }

  override def hashCode: Int = 31 + topicPartition.hashCode + 17 * brokerId

  override def toString: String = {
    val replicaString = new StringBuilder
    replicaString.append("Replica(replicaId=" + brokerId)
    replicaString.append(s", topic=${topicPartition.topic}")
    replicaString.append(s", partition=${topicPartition.partition}")
    replicaString.append(s", isLocal=$isLocal")
    replicaString.append(s", lastCaughtUpTimeMs=$lastCaughtUpTimeMs")
    if (isLocal) {
      replicaString.append(s", highWatermark=$highWatermark")
      replicaString.append(s", lastStableOffset=$lastStableOffset")
    }
    replicaString.append(")")
    replicaString.toString
  }
}
