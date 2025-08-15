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

import kafka.log.LogManager
import kafka.network.SocketServer
import kafka.utils.Logging
import org.apache.kafka.common.ClusterResource
import org.apache.kafka.common.internals.{ClusterResourceListeners, Plugin}
import org.apache.kafka.common.metrics.{Metrics, MetricsReporter}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.Time
import org.apache.kafka.coordinator.group.GroupCoordinator
import org.apache.kafka.metadata.{BrokerState, MetadataCache}
import org.apache.kafka.security.CredentialProvider
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.common.NodeToControllerChannelManager
import org.apache.kafka.server.log.remote.storage.RemoteLogManager
import org.apache.kafka.server.metrics.{KafkaMetricsGroup, KafkaYammerMetrics, LinuxIoMetricsCollector}
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.log.LogDirFailureChannel
import org.apache.kafka.storage.log.metrics.BrokerTopicStats

import java.time.Duration
import scala.collection.Seq
import scala.jdk.CollectionConverters._

/**
 * KafkaBroker 伴生对象，包含 Kafka 代理的静态方法和常量定义。
 * 提供集群监听器通知、指标报告器通知等功能。
 */
object KafkaBroker {
  // 指标上下文的属性
  private val MetricsTypeName: String = "KafkaServer"

  /**
   * 通知集群监听器集群资源的变化。
   * 
   * @param clusterId 集群ID
   * @param clusterListeners 集群监听器列表
   */
  private[server] def notifyClusterListeners(clusterId: String,
                                             clusterListeners: Seq[AnyRef]): Unit = {
    val clusterResourceListeners = new ClusterResourceListeners
    clusterResourceListeners.maybeAddAll(clusterListeners.asJava)
    clusterResourceListeners.onUpdate(new ClusterResource(clusterId))
  }

  /**
   * 通知指标报告器上下文变化。
   * 
   * @param clusterId 集群ID
   * @param config Kafka配置
   * @param metricsReporters 指标报告器列表
   */
  private[server] def notifyMetricsReporters(clusterId: String,
                                             config: KafkaConfig,
                                             metricsReporters: Seq[AnyRef]): Unit = {
    val metricsContext = Server.createKafkaMetricsContext(config, clusterId)
    metricsReporters.foreach {
      case x: MetricsReporter => x.contextChange(metricsContext)
      case _ => // 什么都不做
    }
  }

  /**
   * 当代理成功启动时打印的日志消息。
   * ducktape 系统测试会查找匹配正则表达式 'Kafka\s*Server.*started' 的行
   * 来确定代理是否已启动，因此最好不要更改此消息 -- 但如果
   * 您确实更改了它，请确保它匹配该正则表达式，否则系统测试将失败。
   */
  val STARTED_MESSAGE = "Kafka Server started"

  /**
   * 增量获取会话逐出的最小时间间隔（毫秒）。
   * 用于控制获取会话缓存的清理频率。
   */
  val MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS: Long = 120000
}

/**
 * KafkaBroker 特质定义了 Kafka 代理的核心接口。
 * 这个特质包含了代理运行所需的所有组件和方法，包括：
 * - 网络处理和请求处理
 * - 日志管理和副本管理
 * - 元数据缓存和协调器
 * - 指标收集和配额管理
 * - 安全认证和授权
 * - 生命周期管理（启动、关闭等）
 */
trait KafkaBroker extends Logging {
  // 将 FetchSessionCache 分割成的分片数量。这是为了减少在处理 Fetch 请求时
  // 尝试获取锁时的竞争。
  val NumFetchSessionCacheShards: Int = 8

  /** 授权插件，用于访问控制 */
  def authorizerPlugin: Option[Plugin[Authorizer]]
  
  /** 代理状态，表示当前代理的运行状态 */
  def brokerState: BrokerState
  
  /** 集群ID，唯一标识当前 Kafka 集群 */
  def clusterId: String
  
  /** Kafka 配置对象，包含所有配置参数 */
  def config: KafkaConfig
  
  /** 数据平面请求处理器池，处理客户端请求 */
  def dataPlaneRequestHandlerPool: KafkaRequestHandlerPool
  
  /** 数据平面请求处理器，实际处理各种 API 请求 */
  def dataPlaneRequestProcessor: KafkaApis
  
  /** Kafka 调度器，用于执行定时任务 */
  def kafkaScheduler: Scheduler
  
  /** Kafka Yammer 指标系统 */
  def kafkaYammerMetrics: KafkaYammerMetrics
  
  /** 日志目录故障通道，处理日志目录故障 */
  def logDirFailureChannel: LogDirFailureChannel
  
  /** 日志管理器，管理所有主题分区的日志 */
  def logManager: LogManager
  
  /** 远程日志管理器（可选），用于远程存储 */
  def remoteLogManagerOpt: Option[RemoteLogManager]
  
  /** 指标收集系统 */
  def metrics: Metrics
  
  /** 配额管理器，管理各种资源配额 */
  def quotaManagers: QuotaFactory.QuotaManagers
  
  /** 副本管理器，管理分区副本 */
  def replicaManager: ReplicaManager
  
  /** Socket 服务器，处理网络连接 */
  def socketServer: SocketServer
  
  /** 元数据缓存，缓存集群元数据信息 */
  def metadataCache: MetadataCache
  
  /** 组协调器，管理消费者组 */
  def groupCoordinator: GroupCoordinator
  
  /**
   * 获取指定监听器绑定的端口号。
   * 
   * @param listenerName 监听器名称
   * @return 绑定的端口号
   */
  def boundPort(listenerName: ListenerName): Int
  
  /** 启动代理 */
  def startup(): Unit
  
  /** 等待代理关闭 */
  def awaitShutdown(): Unit
  
  /** 使用默认超时时间（5分钟）关闭代理 */
  def shutdown(): Unit = shutdown(Duration.ofMinutes(5))
  
  /**
   * 在指定超时时间内关闭代理。
   * 
   * @param timeout 关闭超时时间
   */
  def shutdown(timeout: Duration): Unit
  
  /**
   * 检查代理是否已关闭。
   * 
   * @return 如果代理已关闭则返回 true
   */
  def isShutdown(): Boolean
  
  /** 代理主题统计信息 */
  def brokerTopicStats: BrokerTopicStats
  
  /** 凭证提供者，用于安全认证 */
  def credentialProvider: CredentialProvider
  
  /** 客户端到控制器的通道管理器 */
  def clientToControllerChannelManager: NodeToControllerChannelManager
  
  /** 委托令牌缓存，用于安全认证 */
  def tokenCache: DelegationTokenCache

  // For backwards compatibility, we need to keep older metrics tied
  // to their original name when this class was named `KafkaServer`
  private val metricsGroup = new KafkaMetricsGroup(Server.MetricsPrefix, KafkaBroker.MetricsTypeName)

  metricsGroup.newGauge("BrokerState", () => brokerState.value)
  metricsGroup.newGauge("ClusterId", () => clusterId)
  metricsGroup.newGauge("yammer-metrics-count", () =>  KafkaYammerMetrics.defaultRegistry.allMetrics.size)

  private val linuxIoMetricsCollector = new LinuxIoMetricsCollector("/proc", Time.SYSTEM)

  if (linuxIoMetricsCollector.usable()) {
    metricsGroup.newGauge("linux-disk-read-bytes", () => linuxIoMetricsCollector.readBytes())
    metricsGroup.newGauge("linux-disk-write-bytes", () => linuxIoMetricsCollector.writeBytes())
  }
}
