/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.metrics.KafkaMetric;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Kafka生产者接口，定义了向Kafka集群发送消息的所有操作。
 * <p>
 * 生产者负责将消息发送到Kafka集群中的主题，支持同步和异步发送模式。
 * 它还提供了事务支持，允许将多个消息发送操作作为一个原子单元处理。
 * <p>
 * 生产者实例是线程安全的，可以在多个线程中共享使用。
 * <p>
 * 主要功能包括：
 * <ul>
 *   <li>发送消息到Kafka主题</li>
 *   <li>管理事务（开始、提交、中止）</li>
 *   <li>获取主题分区信息</li>
 *   <li>监控和度量指标</li>
 * </ul>
 * 
 * @param <K> 消息键的类型
 * @param <V> 消息值的类型
 * @see KafkaProducer 生产者的主要实现类
 * @see MockProducer 用于测试的模拟实现类
 */
public interface Producer<K, V> extends Closeable {

    /**
     * 初始化事务。
     * 在使用事务功能前，必须先调用此方法。该方法将获取事务协调器并初始化事务状态。
     * 如果配置了transactional.id，则此方法必须在任何其他方法之前调用。
     * 此方法使用默认参数调用{@link #initTransactions(boolean)}，keepPreparedTxn设为false。
     * 
     * @see KafkaProducer#initTransactions()
     */
    default void initTransactions() {
        initTransactions(false);
    }

    /**
     * 初始化事务，并可选择是否保留已准备但未完成的事务。
     * 在使用事务功能前，必须先调用此方法。该方法将获取事务协调器并初始化事务状态。
     * 
     * @param keepPreparedTxn 如果为true，则保留已准备但未完成的事务；如果为false，则中止所有未完成的事务
     * @throws IllegalStateException 如果未配置transactional.id
     * @throws UnsupportedVersionException 如果broker不支持事务（版本低于0.11.0.0）
     * @throws AuthorizationException 如果未被授权访问事务协调器
     * @throws KafkaException 如果初始化事务失败
     * 
     * @see KafkaProducer#initTransactions(boolean)
     */
    void initTransactions(boolean keepPreparedTxn);

    /**
     * 开始一个新的事务。
     * 在调用此方法后，所有通过{@link #send(ProducerRecord)}发送的消息将成为事务的一部分，
     * 直到调用{@link #commitTransaction()}或{@link #abortTransaction()}。
     * 
     * @throws IllegalStateException 如果未配置transactional.id或未调用initTransactions()
     * @throws ProducerFencedException 如果另一个具有相同transactional.id的生产者处于活动状态
     * @throws KafkaException 如果开始事务失败
     * 
     * @see KafkaProducer#beginTransaction()
     */
    void beginTransaction() throws ProducerFencedException;

    /**
     * 在当前事务中提交指定的消费者组偏移量。
     * 此方法允许在一个原子操作中同时提交消费者的偏移量和生产者的事务，实现精确一次处理语义。
     * 必须在{@link #beginTransaction()}之后、{@link #commitTransaction()}之前调用。
     * 
     * @param offsets 要提交的主题分区偏移量映射
     * @param groupMetadata 消费者组的元数据信息
     * @throws IllegalStateException 如果未配置transactional.id或未开始事务
     * @throws ProducerFencedException 如果另一个具有相同transactional.id的生产者处于活动状态
     * @throws KafkaException 如果提交偏移量失败
     * 
     * @see KafkaProducer#sendOffsetsToTransaction(Map, ConsumerGroupMetadata)
     */
    void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                  ConsumerGroupMetadata groupMetadata) throws ProducerFencedException;

    /**
     * 准备当前事务进行两阶段提交。
     * 此方法将刷新所有待处理的消息，并将生产者转换为一种状态，在该状态下只能调用
     * {@link #commitTransaction()}、{@link #abortTransaction()}或
     * {@link #completeTransaction(PreparedTxnState)}。
     * 
     * 此方法用作两阶段提交协议的一部分：
     * 1. 通过调用此方法准备事务，成功时返回{@link PreparedTxnState}
     * 2. 进行需要与此事务原子性关联的外部系统更改
     * 3. 通过调用{@link #commitTransaction()}、{@link #abortTransaction()}或
     *    {@link #completeTransaction(PreparedTxnState)}完成事务
     * 
     * @return 用于完成事务的准备状态对象
     * @throws IllegalStateException 如果未配置transactional.id或未开始事务
     * @throws InvalidTxnStateException 如果生产者不处于可准备事务的状态或未启用两阶段提交
     * @throws ProducerFencedException 如果另一个具有相同transactional.id的生产者处于活动状态
     * @throws UnsupportedVersionException 如果broker不支持事务（版本低于0.11.0.0）
     * 
     * @see KafkaProducer#prepareTransaction()
     */
    PreparedTxnState prepareTransaction() throws ProducerFencedException;

    /**
     * 提交当前事务。
     * 此方法将刷新所有待处理的消息，并将事务标记为已提交。事务中的所有消息将对消费者可见。
     * 
     * @throws IllegalStateException 如果未配置transactional.id或未开始事务
     * @throws ProducerFencedException 如果另一个具有相同transactional.id的生产者处于活动状态
     * @throws KafkaException 如果提交事务失败
     * 
     * @see KafkaProducer#commitTransaction()
     */
    void commitTransaction() throws ProducerFencedException;

    /**
     * 中止当前事务。
     * 此方法将丢弃事务中的所有消息，并将事务标记为已中止。事务中的所有消息将对消费者不可见。
     * 
     * @throws IllegalStateException 如果未配置transactional.id或未开始事务
     * @throws ProducerFencedException 如果另一个具有相同transactional.id的生产者处于活动状态
     * @throws KafkaException 如果中止事务失败
     * 
     * @see KafkaProducer#abortTransaction()
     */
    void abortTransaction() throws ProducerFencedException;

    /**
     * 使用之前准备的事务状态完成事务。
     * 此方法用于两阶段提交协议的最后一步，根据提供的准备状态决定提交或中止事务。
     * 
     * @param preparedTxnState 由{@link #prepareTransaction()}返回的准备事务状态
     * @throws IllegalStateException 如果未配置transactional.id或未开始事务
     * @throws ProducerFencedException 如果另一个具有相同transactional.id的生产者处于活动状态
     * @throws KafkaException 如果完成事务失败
     * 
     * @see KafkaProducer#completeTransaction(PreparedTxnState)
     */
    void completeTransaction(PreparedTxnState preparedTxnState) throws ProducerFencedException;

    /**
     * 为订阅注册度量指标。
     * 此方法用于注册与生产者订阅相关的度量指标，以便进行监控和性能分析。
     * 
     * @param metric 要注册的Kafka度量指标
     * @see KafkaProducer#registerMetricForSubscription(KafkaMetric) 
     */
    void registerMetricForSubscription(KafkaMetric metric);

    /**
     * 从订阅中注销度量指标。
     * 此方法用于从生产者订阅中移除之前注册的度量指标。
     * 
     * @param metric 要注销的Kafka度量指标
     * @see KafkaProducer#unregisterMetricFromSubscription(KafkaMetric) 
     */
    void unregisterMetricFromSubscription(KafkaMetric metric);

    /**
     * 异步发送消息到Kafka集群。
     * 此方法将消息添加到发送队列，并立即返回。实际发送操作是异步执行的。
     * 返回的Future对象可用于获取发送结果。
     * 
     * @param record 要发送的消息记录
     * @return 包含发送结果的Future对象
     * @throws AuthenticationException 如果身份验证失败
     * @throws AuthorizationException 如果未被授权访问主题
     * @throws IllegalStateException 如果生产者已关闭
     * @throws InterruptException 如果线程被中断
     * @throws SerializationException 如果序列化消息失败
     * @throws TimeoutException 如果发送超时
     * @throws KafkaException 如果发送失败
     * 
     * @see KafkaProducer#send(ProducerRecord)
     */
    Future<RecordMetadata> send(ProducerRecord<K, V> record);

    /**
     * 异步发送消息到Kafka集群，并在发送完成时执行回调。
     * 此方法将消息添加到发送队列，并立即返回。实际发送操作是异步执行的。
     * 当发送完成时，将调用提供的回调函数。
     * 
     * @param record 要发送的消息记录
     * @param callback 发送完成时执行的回调函数
     * @return 包含发送结果的Future对象
     * @throws AuthenticationException 如果身份验证失败
     * @throws AuthorizationException 如果未被授权访问主题
     * @throws IllegalStateException 如果生产者已关闭
     * @throws InterruptException 如果线程被中断
     * @throws SerializationException 如果序列化消息失败
     * @throws TimeoutException 如果发送超时
     * @throws KafkaException 如果发送失败
     * 
     * @see KafkaProducer#send(ProducerRecord, Callback)
     */
    Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback);

    /**
     * 刷新所有待处理的消息。
     * 此方法将阻塞，直到所有当前待处理的消息都被发送到服务器，并收到确认（如果需要）。
     * 
     * @throws InterruptException 如果线程被中断
     * @throws KafkaException 如果刷新失败
     * 
     * @see KafkaProducer#flush()
     */
    void flush();

    /**
     * 获取指定主题的分区信息。
     * 此方法返回主题的所有分区信息，包括分区ID、leader副本、副本列表等。
     * 
     * @param topic 要查询的主题名称
     * @return 主题分区信息列表
     * @throws AuthenticationException 如果身份验证失败
     * @throws AuthorizationException 如果未被授权访问主题
     * @throws TimeoutException 如果查询超时
     * @throws KafkaException 如果查询失败
     * 
     * @see KafkaProducer#partitionsFor(String)
     */
    List<PartitionInfo> partitionsFor(String topic);

    /**
     * 获取生产者的度量指标。
     * 此方法返回生产者的各种性能指标，如消息发送速率、延迟、错误率等。
     * 
     * @return 度量指标的映射，键为指标名称，值为指标对象
     * 
     * @see KafkaProducer#metrics()
     */
    Map<MetricName, ? extends Metric> metrics();

    /**
     * 获取生产者的客户端实例ID。
     * 此方法返回一个唯一标识符，用于标识此生产者实例。
     * 
     * @param timeout 等待获取实例ID的超时时间
     * @return 客户端实例的唯一ID
     * @throws TimeoutException 如果在指定的超时时间内未能获取实例ID
     * @throws KafkaException 如果获取实例ID失败
     * 
     * @see KafkaProducer#clientInstanceId(Duration)}
     */
    Uuid clientInstanceId(Duration timeout);

    /**
     * 关闭生产者。
     * 此方法将阻塞，直到所有待处理的消息都被发送到服务器，并释放所有资源。
     * 使用默认超时时间。
     * 
     * @throws InterruptException 如果线程被中断
     * @throws KafkaException 如果关闭失败
     * 
     * @see KafkaProducer#close()
     */
    void close();

    /**
     * 关闭生产者，并指定超时时间。
     * 此方法将阻塞，直到所有待处理的消息都被发送到服务器，或者达到指定的超时时间，然后释放所有资源。
     * 
     * @param timeout 等待所有待处理消息发送完成的最大时间
     * @throws InterruptException 如果线程被中断
     * @throws KafkaException 如果关闭失败
     * 
     * @see KafkaProducer#close(Duration)
     */
    void close(Duration timeout);
}
