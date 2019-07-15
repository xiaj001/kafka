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

package kafka.server

import kafka.network._
import kafka.utils._
import kafka.metrics.KafkaMetricsGroup
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import com.yammer.metrics.core.Meter
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.utils.{KafkaThread, Time}

import scala.collection.mutable

/**
 * A thread that answers kafka requests.
  * KafkaRequestHandler 的主要职责是从 RequestChannel中获取请求，
  * 并调用KafkaApis.handle() 方法处理请求
 */
class KafkaRequestHandler(id: Int,
                          brokerId: Int,
                          val aggregateIdleMeter: Meter,
                          val totalHandlerThreads: AtomicInteger,
                          val requestChannel: RequestChannel,
                          apis: KafkaApis,
                          time: Time) extends Runnable with Logging {
  this.logIdent = "[Kafka Request Handler " + id + " on Broker " + brokerId + "], "
  private val shutdownComplete = new CountDownLatch(1)
  @volatile private var stopped = false

  def run() {
    while (!stopped) {
      // We use a single meter for aggregate idle percentage for the thread pool.
      // Since meter is calculated as total_recorded_value / time_window and
      // time_window is independent of the number of threads, each recorded idle
      // time should be discounted by # threads.
      val startSelectTime = time.nanoseconds

      // 从RequestChannel.requestQueue获取请求，通过requestQueue.poll() 方法实现
      val req = requestChannel.receiveRequest(300)
      val endTime = time.nanoseconds
      val idleTime = endTime - startSelectTime
      // 统计监控指标
      aggregateIdleMeter.mark(idleTime / totalHandlerThreads.get)

      req match {
        case RequestChannel.ShutdownRequest =>
          debug(s"Kafka request handler $id on broker $brokerId received shut down command")
          shutdownComplete.countDown()
          return

        case request: RequestChannel.Request =>
          try {
            request.requestDequeueTimeNanos = endTime
            trace(s"Kafka request handler $id on broker $brokerId handling request $request")

            //KafkaApis类中实现了处理请求的逻辑，KafkaApis 还负责将响应
            // 写回到对应的RequestChannel.responseQueue中，唤醒Processor处理
            apis.handle(request)
          } catch {
            case e: FatalExitError =>
              shutdownComplete.countDown()
              Exit.exit(e.statusCode)
            case e: Throwable => error("Exception when handling request", e)
          } finally {
            request.releaseBuffer()
          }

        case null => // continue
      }
    }
    shutdownComplete.countDown()
  }

  def stop(): Unit = {
    stopped = true
  }

  def initiateShutdown(): Unit = requestChannel.sendShutdownRequest()

  def awaitShutdown(): Unit = shutdownComplete.await()

}

/**
  * API层使用KafkaRequestHandlerPool来管理所有的KafkaRequestHandler 线程，
  * 它是一个简易版的线程池，其中创建了多个KafkaRequestHandler线程
  * @param brokerId
  * @param requestChannel
  * @param apis
  * @param time
  * @param numThreads
  */
class KafkaRequestHandlerPool(val brokerId: Int,
                              val requestChannel: RequestChannel,
                              val apis: KafkaApis,
                              time: Time,
                              numThreads: Int) extends Logging with KafkaMetricsGroup {

  private val threadPoolSize: AtomicInteger = new AtomicInteger(numThreads)
  /* a meter to track the average free capacity of the request handlers */
  private val aggregateIdleMeter = newMeter("RequestHandlerAvgIdlePercent", "percent", TimeUnit.NANOSECONDS)

  this.logIdent = "[Kafka Request Handler on Broker " + brokerId + "], "

  //用于保存执行KafkaRequestHandler的线程
  val runnables = new mutable.ArrayBuffer[KafkaRequestHandler](numThreads)

  //创建KafkaRequestHandler线程
  for (i <- 0 until numThreads) {
    createHandler(i)
  }

  // 创建并启动 KafkaRequestHandler 线程
  def createHandler(id: Int): Unit = synchronized {
    runnables += new KafkaRequestHandler(id, brokerId, aggregateIdleMeter, threadPoolSize, requestChannel, apis, time)
    KafkaThread.daemon("kafka-request-handler-" + id, runnables(id)).start()
  }

  //重新设置线程池大小
  def resizeThreadPool(newSize: Int): Unit = synchronized {
    val currentSize = threadPoolSize.get
    info(s"Resizing request handler thread pool size from $currentSize to $newSize")
    if (newSize > currentSize) {
      for (i <- currentSize until newSize) {
        createHandler(i)
      }
    } else if (newSize < currentSize) {
      for (i <- 1 to (currentSize - newSize)) {
        runnables.remove(currentSize - i).stop()
      }
    }
    threadPoolSize.set(newSize)
  }

  def shutdown(): Unit = synchronized {
    info("shutting down")
    for (handler <- runnables)
      handler.initiateShutdown()
    for (handler <- runnables)
      handler.awaitShutdown()
    info("shut down completely")
  }
}

class BrokerTopicMetrics(name: Option[String]) extends KafkaMetricsGroup {
  val tags: scala.collection.Map[String, String] = name match {
    case None => Map.empty
    case Some(topic) => Map("topic" -> topic)
  }

  val messagesInRate = newMeter(BrokerTopicStats.MessagesInPerSec, "messages", TimeUnit.SECONDS, tags)
  val bytesInRate = newMeter(BrokerTopicStats.BytesInPerSec, "bytes", TimeUnit.SECONDS, tags)
  val bytesOutRate = newMeter(BrokerTopicStats.BytesOutPerSec, "bytes", TimeUnit.SECONDS, tags)
  val bytesRejectedRate = newMeter(BrokerTopicStats.BytesRejectedPerSec, "bytes", TimeUnit.SECONDS, tags)
  private[server] val replicationBytesInRate =
    if (name.isEmpty) Some(newMeter(BrokerTopicStats.ReplicationBytesInPerSec, "bytes", TimeUnit.SECONDS, tags))
    else None
  private[server] val replicationBytesOutRate =
    if (name.isEmpty) Some(newMeter(BrokerTopicStats.ReplicationBytesOutPerSec, "bytes", TimeUnit.SECONDS, tags))
    else None
  val failedProduceRequestRate = newMeter(BrokerTopicStats.FailedProduceRequestsPerSec, "requests", TimeUnit.SECONDS, tags)
  val failedFetchRequestRate = newMeter(BrokerTopicStats.FailedFetchRequestsPerSec, "requests", TimeUnit.SECONDS, tags)
  val totalProduceRequestRate = newMeter(BrokerTopicStats.TotalProduceRequestsPerSec, "requests", TimeUnit.SECONDS, tags)
  val totalFetchRequestRate = newMeter(BrokerTopicStats.TotalFetchRequestsPerSec, "requests", TimeUnit.SECONDS, tags)
  val fetchMessageConversionsRate = newMeter(BrokerTopicStats.FetchMessageConversionsPerSec, "requests", TimeUnit.SECONDS, tags)
  val produceMessageConversionsRate = newMeter(BrokerTopicStats.ProduceMessageConversionsPerSec, "requests", TimeUnit.SECONDS, tags)

  def close() {
    removeMetric(BrokerTopicStats.MessagesInPerSec, tags)
    removeMetric(BrokerTopicStats.BytesInPerSec, tags)
    removeMetric(BrokerTopicStats.BytesOutPerSec, tags)
    removeMetric(BrokerTopicStats.BytesRejectedPerSec, tags)
    if (replicationBytesInRate.isDefined)
      removeMetric(BrokerTopicStats.ReplicationBytesInPerSec, tags)
    if (replicationBytesOutRate.isDefined)
      removeMetric(BrokerTopicStats.ReplicationBytesOutPerSec, tags)
    removeMetric(BrokerTopicStats.FailedProduceRequestsPerSec, tags)
    removeMetric(BrokerTopicStats.FailedFetchRequestsPerSec, tags)
    removeMetric(BrokerTopicStats.TotalProduceRequestsPerSec, tags)
    removeMetric(BrokerTopicStats.TotalFetchRequestsPerSec, tags)
    removeMetric(BrokerTopicStats.FetchMessageConversionsPerSec, tags)
    removeMetric(BrokerTopicStats.ProduceMessageConversionsPerSec, tags)
  }
}

object BrokerTopicStats {
  val MessagesInPerSec = "MessagesInPerSec"
  val BytesInPerSec = "BytesInPerSec"
  val BytesOutPerSec = "BytesOutPerSec"
  val BytesRejectedPerSec = "BytesRejectedPerSec"
  val ReplicationBytesInPerSec = "ReplicationBytesInPerSec"
  val ReplicationBytesOutPerSec = "ReplicationBytesOutPerSec"
  val FailedProduceRequestsPerSec = "FailedProduceRequestsPerSec"
  val FailedFetchRequestsPerSec = "FailedFetchRequestsPerSec"
  val TotalProduceRequestsPerSec = "TotalProduceRequestsPerSec"
  val TotalFetchRequestsPerSec = "TotalFetchRequestsPerSec"
  val FetchMessageConversionsPerSec = "FetchMessageConversionsPerSec"
  val ProduceMessageConversionsPerSec = "ProduceMessageConversionsPerSec"
  private val valueFactory = (k: String) => new BrokerTopicMetrics(Some(k))
}

class BrokerTopicStats {
  import BrokerTopicStats._

  private val stats = new Pool[String, BrokerTopicMetrics](Some(valueFactory))
  val allTopicsStats = new BrokerTopicMetrics(None)

  def topicStats(topic: String): BrokerTopicMetrics =
    stats.getAndMaybePut(topic)

  def updateReplicationBytesIn(value: Long) {
    allTopicsStats.replicationBytesInRate.foreach { metric =>
      metric.mark(value)
    }
  }

  private def updateReplicationBytesOut(value: Long) {
    allTopicsStats.replicationBytesOutRate.foreach { metric =>
      metric.mark(value)
    }
  }

  def removeMetrics(topic: String) {
    val metrics = stats.remove(topic)
    if (metrics != null)
      metrics.close()
  }

  def updateBytesOut(topic: String, isFollower: Boolean, value: Long) {
    if (isFollower) {
      updateReplicationBytesOut(value)
    } else {
      topicStats(topic).bytesOutRate.mark(value)
      allTopicsStats.bytesOutRate.mark(value)
    }
  }


  def close(): Unit = {
    allTopicsStats.close()
    stats.values.foreach(_.close())
  }

}
