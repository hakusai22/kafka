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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.KafkaMetric;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Kafka 消费者接口，定义了从 Kafka 集群消费消息的核心功能。
 * <p>
 * 该接口提供了两种主要的消费模式：
 * <ul>
 * <li>基于主题订阅的自动分区分配模式 - 使用 {@link #subscribe(Collection)} 方法</li>
 * <li>手动分区分配模式 - 使用 {@link #assign(Collection)} 方法</li>
 * </ul>
 * <p>
 * 消费者支持以下核心功能：
 * <ul>
 * <li>消息拉取和消费</li>
 * <li>偏移量管理（同步和异步提交）</li>
 * <li>分区重平衡处理</li>
 * <li>消费位置控制（seek 操作）</li>
 * <li>元数据查询</li>
 * <li>度量指标收集</li>
 * </ul>
 * 
 * @param <K> 消息键的类型
 * @param <V> 消息值的类型
 * @see KafkaConsumer 主要实现类
 * @see MockConsumer 测试用的模拟实现
 */
public interface Consumer<K, V> extends Closeable {

    /**
     * 获取当前分配给此消费者的分区集合。
     * <p>
     * 如果使用 {@link #assign(Collection)} 直接分配分区，则返回相同的分区集合。
     * 如果使用主题订阅，则返回当前分配给消费者的主题分区集合（如果尚未发生分配或分区正在重新分配过程中，可能为空）。
     * 
     * @return 当前分配给此消费者的分区集合
     * @see KafkaConsumer#assignment()
     */
    Set<TopicPartition> assignment();

    /**
     * 获取当前的主题订阅。
     * <p>
     * 返回最近一次调用 {@link #subscribe(Collection, ConsumerRebalanceListener)} 时使用的相同主题，
     * 如果没有进行过此类调用，则返回空集合。
     * 
     * @return 当前订阅的主题集合
     * @see KafkaConsumer#subscription()
     */
    Set<String> subscription();

    /**
     * 订阅给定的主题列表以获得动态分配的分区。
     * <p>
     * <b>主题订阅不是增量的。此列表将替换当前的分配（如果有的话）。</b>
     * 注意，不能将基于主题订阅的组管理与通过 {@link #assign(Collection)} 进行的手动分区分配结合使用。
     * <p>
     * 如果给定的主题列表为空，则与 {@link #unsubscribe()} 的处理方式相同。
     * 
     * @param topics 要订阅的主题列表
     * @throws IllegalArgumentException 如果 topics 为 null 或包含 null 或空元素
     * @throws IllegalStateException 如果之前使用模式调用了 {@code subscribe()}，或之前调用了 assign（没有后续调用 {@link #unsubscribe()}），或者没有配置至少一个分区分配策略
     * @see KafkaConsumer#subscribe(Collection)
     */
    void subscribe(Collection<String> topics);

    /**
     * 订阅给定的主题列表以获得动态分配的分区。
     * <p>
     * <b>主题订阅不是增量的。此列表将替换当前的分配（如果有的话）。</b>
     * 注意，不能将基于主题订阅的组管理与通过 {@link #assign(Collection)} 进行的手动分区分配结合使用。
     * <p>
     * 作为组管理的一部分，消费者将跟踪属于特定组的消费者列表，并在触发以下任何事件时触发重平衡操作：
     * <ul>
     * <li>任何订阅主题的分区数发生变化</li>
     * <li>订阅的主题被创建或删除</li>
     * <li>消费者组的现有成员关闭或失败</li>
     * <li>新成员加入消费者组</li>
     * </ul>
     * 
     * @param topics 要订阅的主题列表
     * @param callback 非空的监听器实例，用于获取订阅主题的分区分配/撤销通知
     * @throws IllegalArgumentException 如果 topics 为 null 或包含 null 或空元素，或者 callback 为 null
     * @throws IllegalStateException 如果之前使用模式调用了 {@code subscribe()}，或之前调用了 assign（没有后续调用 {@link #unsubscribe()}），或者没有配置至少一个分区分配策略
     * @see KafkaConsumer#subscribe(Collection, ConsumerRebalanceListener)
     */
    void subscribe(Collection<String> topics, ConsumerRebalanceListener callback);

    /**
     * 手动将分区列表分配给此消费者。
     * <p>
     * 此接口不允许增量分配，将替换之前的分配（如果有的话）。
     * 如果给定的主题分区列表为空，则与 {@link #unsubscribe()} 的处理方式相同。
     * <p>
     * 通过此方法进行的手动主题分配不使用消费者的组管理功能。
     * 因此，当组成员或集群和主题元数据发生变化时，不会触发重平衡操作。
     * 注意，不能同时使用 {@link #assign(Collection)} 的手动分区分配和 {@link #subscribe(Collection, ConsumerRebalanceListener)} 的组分配。
     * 
     * @param partitions 要分配给此消费者的分区列表
     * @throws IllegalArgumentException 如果 partitions 为 null 或包含 null 或空主题
     * @throws IllegalStateException 如果之前使用主题或模式调用了 {@code subscribe()}（没有后续调用 {@link #unsubscribe()}）
     * @see KafkaConsumer#assign(Collection)
     */
    void assign(Collection<TopicPartition> partitions);

    /**
     * 订阅所有匹配指定模式的主题以获得动态分配的分区。
     * <p>
     * 模式匹配将定期对检查时存在的所有主题进行。
     * 这可以通过 {@code metadata.max.age.ms} 配置来控制：通过降低最大元数据年龄，消费者将更频繁地刷新元数据并检查匹配的主题。
     * <p>
     * 有关 {@link ConsumerRebalanceListener} 使用的详细信息，请参见 {@link #subscribe(Collection, ConsumerRebalanceListener)}。
     * 通常，当提供的模式匹配的主题发生变化以及消费者组成员发生变化时，会触发重平衡。
     * 组重平衡仅在活动调用 {@link #poll(Duration)} 期间发生。
     * 
     * @param pattern 要订阅的模式
     * @param callback 非空的监听器实例，用于获取订阅主题的分区分配/撤销通知
     * @throws IllegalArgumentException 如果 pattern 或 callback 为 null
     * @throws IllegalStateException 如果之前使用主题调用了 {@code subscribe()}，或之前调用了 assign（没有后续调用 {@link #unsubscribe()}），或者没有配置至少一个分区分配策略
     * @see KafkaConsumer#subscribe(Pattern, ConsumerRebalanceListener)
     */
    void subscribe(Pattern pattern, ConsumerRebalanceListener callback);

    /**
     * 订阅所有匹配指定模式的主题以获得动态分配的分区。
     * <p>
     * 模式匹配将定期对检查时存在的主题进行。
     * <p>
     * 这是 {@link #subscribe(Pattern, ConsumerRebalanceListener)} 的简化版本，使用无操作监听器。
     * 如果您需要寻址到特定偏移量的能力，应该优先使用 {@link #subscribe(Pattern, ConsumerRebalanceListener)}，
     * 因为组重平衡会导致分区偏移量被重置。如果您正在进行自己的偏移量管理，也应该提供自己的监听器，
     * 因为监听器为您提供了在重平衡完成之前提交偏移量的机会。
     * 
     * @param pattern 要订阅的模式
     * @throws IllegalArgumentException 如果 pattern 为 null
     * @throws IllegalStateException 如果之前使用主题调用了 {@code subscribe()}，或之前调用了 assign（没有后续调用 {@link #unsubscribe()}），或者没有配置至少一个分区分配策略
     * @see KafkaConsumer#subscribe(Pattern)
     */
    void subscribe(Pattern pattern);

    /**
     * 订阅所有匹配指定模式的主题，以获得动态分配的分区。
     * <p>
     * 模式匹配将定期对所有主题进行。这仅在 CONSUMER 组协议下受支持（参见 {@link ConsumerConfig#GROUP_PROTOCOL_CONFIG}）。
     * <p>
     * 如果提供的模式与 Google RE2/J 不兼容，最终会在调用此订阅后的 {@link #poll(Duration)} 调用中抛出 {@link InvalidRegularExpression}。
     * 
     * @param pattern 要订阅的模式，必须与 Google RE2/J 兼容
     * @param callback 非空的监听器实例，用于获取订阅主题的分区分配/撤销通知
     * @throws IllegalArgumentException 如果 pattern 为 null 或空，或者 callback 为 null
     * @throws IllegalStateException 如果之前使用主题调用了 {@code subscribe()}，或之前调用了 assign（没有后续调用 {@link #unsubscribe()}）
     * @see KafkaConsumer#subscribe(SubscriptionPattern, ConsumerRebalanceListener)
     */
    void subscribe(SubscriptionPattern pattern, ConsumerRebalanceListener callback);

    /**
     * 订阅所有匹配指定模式的主题，以获得动态分配的分区。
     * <p>
     * 模式匹配将定期对主题进行。这仅在 CONSUMER 组协议下受支持（参见 {@link ConsumerConfig#GROUP_PROTOCOL_CONFIG}）。
     * <p>
     * 如果提供的模式与 Google RE2/J 不兼容，最终会在调用此订阅后的 {@link #poll(Duration)} 调用中抛出 {@link InvalidRegularExpression}。
     * 
     * @param pattern 要订阅的模式，必须与 Google RE2/J 兼容
     * @throws IllegalArgumentException 如果 pattern 为 null 或空
     * @throws IllegalStateException 如果之前使用主题调用了 {@code subscribe()}，或之前调用了 assign（没有后续调用 {@link #unsubscribe()}）
     * @see KafkaConsumer#subscribe(SubscriptionPattern)
     */
    void subscribe(SubscriptionPattern pattern);

    /**
     * 取消订阅当前通过 {@link #subscribe(Collection)} 或 {@link #subscribe(Pattern)} 订阅的主题。
     * <p>
     * 这也会清除通过 {@link #assign(Collection)} 直接分配的任何分区。
     * 
     * @throws org.apache.kafka.common.KafkaException 对于任何其他不可恢复的错误（例如重平衡回调错误）
     * @see KafkaConsumer#unsubscribe()
     */
    void unsubscribe();

    /**
     * 获取使用订阅/分配 API 指定的主题或分区的数据。
     * <p>
     * 在订阅任何主题或分区之前轮询数据是错误的。
     * <p>
     * 在每次轮询时，消费者将尝试使用最后消费的偏移量作为起始偏移量并按顺序获取。
     * 最后消费的偏移量可以通过 {@link #seek(TopicPartition, long)} 手动设置，
     * 或自动设置为订阅分区列表的最后提交偏移量。
     * <p>
     * 如果有可用记录或当 isolation.level=read_committed 时位置超过控制记录或中止事务，此方法立即返回。
     * 否则，它将等待传递的超时时间。如果超时到期，将返回空记录集。
     * 注意，此方法可能会超出超时时间以执行自定义 {@link ConsumerRebalanceListener} 回调。
     * 
     * @param timeout 最大阻塞时间（不得大于 {@link Long#MAX_VALUE} 毫秒）
     * @return 自上次获取订阅主题和分区列表以来的主题到记录的映射
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException 如果分区或分区集的偏移量未定义或超出范围且未配置偏移量重置策略
     * @throws org.apache.kafka.common.errors.WakeupException 如果在调用此函数之前或期间调用了 {@link #wakeup()}
     * @throws org.apache.kafka.common.errors.InterruptException 如果调用线程在调用此函数之前或期间被中断
     * @throws org.apache.kafka.common.errors.AuthenticationException 如果身份验证失败
     * @throws org.apache.kafka.common.errors.AuthorizationException 如果调用者缺乏对任何订阅主题或配置的 groupId 的读取访问权限
     * @throws org.apache.kafka.common.KafkaException 对于任何其他不可恢复的错误
     * @throws java.lang.IllegalArgumentException 如果超时值为负
     * @throws java.lang.IllegalStateException 如果消费者未订阅任何主题或手动分配任何分区进行消费
     * @throws java.lang.ArithmeticException 如果超时大于 {@link Long#MAX_VALUE} 毫秒
     * @see KafkaConsumer#poll(Duration)
     */
    ConsumerRecords<K, V> poll(Duration timeout);

    /**
     * 为所有订阅的主题和分区列表提交最后一次 {@link #poll(Duration) poll()} 返回的偏移量。
     * <p>
     * 这是一个同步提交，将阻塞直到偏移量提交成功或发生不可恢复的错误（在这种情况下会抛出异常）。
     * <p>
     * 注意，异步偏移量提交通常是首选，因为它们具有更好的性能特征。
     * 使用 {@link #commitAsync()} 进行异步提交。
     * 
     * @throws org.apache.kafka.clients.consumer.CommitFailedException 如果提交由于不可恢复的错误而失败
     * @throws org.apache.kafka.common.errors.WakeupException 如果在调用此函数之前或期间调用了 {@link #wakeup()}
     * @throws org.apache.kafka.common.errors.InterruptException 如果调用线程在调用此函数之前或期间被中断
     * @throws org.apache.kafka.common.errors.AuthenticationException 如果身份验证失败
     * @throws org.apache.kafka.common.errors.AuthorizationException 如果调用者缺乏对配置的 groupId 的写入访问权限
     * @throws org.apache.kafka.common.KafkaException 对于任何其他不可恢复的错误
     * @see KafkaConsumer#commitSync()
     */
    void commitSync();

    /**
     * 为所有订阅的主题和分区列表提交最后一次 {@link #poll(Duration) poll()} 返回的偏移量。
     * <p>
     * 这是一个同步提交，将阻塞直到偏移量提交成功、发生不可恢复的错误（在这种情况下会抛出异常）或超时到期。
     * 
     * @param timeout 等待提交完成的最大时间
     * @throws org.apache.kafka.clients.consumer.CommitFailedException 如果提交由于不可恢复的错误而失败
     * @throws org.apache.kafka.common.errors.WakeupException 如果在调用此函数之前或期间调用了 {@link #wakeup()}
     * @throws org.apache.kafka.common.errors.InterruptException 如果调用线程在调用此函数之前或期间被中断
     * @throws org.apache.kafka.common.errors.AuthenticationException 如果身份验证失败
     * @throws org.apache.kafka.common.errors.AuthorizationException 如果调用者缺乏对配置的 groupId 的写入访问权限
     * @throws org.apache.kafka.common.KafkaException 对于任何其他不可恢复的错误
     * @see KafkaConsumer#commitSync(Duration)
     */
    void commitSync(Duration timeout);

    /**
     * 提交指定的偏移量列表给定的分区。
     * <p>
     * 这是一个同步提交，将阻塞直到偏移量提交成功或发生不可恢复的错误（在这种情况下会抛出异常）。
     * 
     * @param offsets 要提交的分区偏移量映射
     * @throws org.apache.kafka.clients.consumer.CommitFailedException 如果提交由于不可恢复的错误而失败
     * @throws org.apache.kafka.common.errors.WakeupException 如果在调用此函数之前或期间调用了 {@link #wakeup()}
     * @throws org.apache.kafka.common.errors.InterruptException 如果调用线程在调用此函数之前或期间被中断
     * @throws org.apache.kafka.common.errors.AuthenticationException 如果身份验证失败
     * @throws org.apache.kafka.common.errors.AuthorizationException 如果调用者缺乏对配置的 groupId 的写入访问权限
     * @throws org.apache.kafka.common.KafkaException 对于任何其他不可恢复的错误
     * @see KafkaConsumer#commitSync(Map)
     */
    void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets);

    /**
     * 提交指定的偏移量列表给定的分区。
     * <p>
     * 这是一个同步提交，将阻塞直到偏移量提交成功、发生不可恢复的错误（在这种情况下会抛出异常）或超时到期。
     * 
     * @param offsets 要提交的分区偏移量映射
     * @param timeout 等待提交完成的最大时间
     * @throws org.apache.kafka.clients.consumer.CommitFailedException 如果提交由于不可恢复的错误而失败
     * @throws org.apache.kafka.common.errors.WakeupException 如果在调用此函数之前或期间调用了 {@link #wakeup()}
     * @throws org.apache.kafka.common.errors.InterruptException 如果调用线程在调用此函数之前或期间被中断
     * @throws org.apache.kafka.common.errors.AuthenticationException 如果身份验证失败
     * @throws org.apache.kafka.common.errors.AuthorizationException 如果调用者缺乏对配置的 groupId 的写入访问权限
     * @throws org.apache.kafka.common.KafkaException 对于任何其他不可恢复的错误
     * @see KafkaConsumer#commitSync(Map, Duration)
     */
    void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout);
    /**
     * 为所有订阅的主题和分区列表提交最后一次 {@link #poll(Duration) poll()} 返回的偏移量。
     * <p>
     * 这是一个异步调用，将立即返回。不保证提交会成功。
     * 使用 {@link #commitAsync(OffsetCommitCallback)} 提供回调以在提交完成时收到通知。
     * 
     * @see KafkaConsumer#commitAsync()
     */
    void commitAsync();

    /**
     * 为所有订阅的主题和分区列表提交最后一次 {@link #poll(Duration) poll()} 返回的偏移量。
     * <p>
     * 这是一个异步调用，将立即返回。不保证提交会成功。
     * 提供的回调将在提交完成时被调用。
     * 
     * @param callback 提交完成时要调用的回调
     * @see KafkaConsumer#commitAsync(OffsetCommitCallback)
     */
    void commitAsync(OffsetCommitCallback callback);

    /**
     * 提交指定的偏移量列表给定的分区。
     * <p>
     * 这是一个异步调用，将立即返回。不保证提交会成功。
     * 提供的回调将在提交完成时被调用。
     * 
     * @param offsets 要提交的分区偏移量映射
     * @param callback 提交完成时要调用的回调
     * @see KafkaConsumer#commitAsync(Map, OffsetCommitCallback)
     */
    void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback);

    /**
     * 为订阅注册一个度量指标
     * @see KafkaConsumer#registerMetricForSubscription(KafkaMetric)
     */
    void registerMetricForSubscription(KafkaMetric metric);

    /**
     * 从订阅中注销一个度量指标
     * @see KafkaConsumer#unregisterMetricFromSubscription(KafkaMetric)
     */
    void unregisterMetricFromSubscription(KafkaMetric metric);

    /**
     * 将消费者的位置重置到指定的偏移量
     * @see KafkaConsumer#seek(TopicPartition, long)
     */
    void seek(TopicPartition partition, long offset);

    /**
     * 将消费者的位置重置到指定的偏移量和元数据
     * @see KafkaConsumer#seek(TopicPartition, OffsetAndMetadata)
     */
    void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata);

    /**
     * 将消费者的位置重置到分区的起始位置
     * @see KafkaConsumer#seekToBeginning(Collection)
     */
    void seekToBeginning(Collection<TopicPartition> partitions);

    /**
     * 将消费者的位置重置到分区的末尾位置
     * @see KafkaConsumer#seekToEnd(Collection)
     */
    void seekToEnd(Collection<TopicPartition> partitions);

    /**
     * 获取消费者在指定分区的当前位置
     * @see KafkaConsumer#position(TopicPartition)
     */
    long position(TopicPartition partition);
    
    /**
     * 在指定超时时间内获取消费者在指定分区的当前位置
     * @see KafkaConsumer#position(TopicPartition, Duration)
     */
    long position(TopicPartition partition, final Duration timeout);

    /**
     * 获取指定分区集合的已提交偏移量
     * @see KafkaConsumer#committed(Set)
     */
    Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions);

    /**
     * 在指定超时时间内获取指定分区集合的已提交偏移量
     * @see KafkaConsumer#committed(Set, Duration)
     */
    Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions, final Duration timeout);

    /**
     * 获取客户端实例ID
     * See {@link KafkaConsumer#clientInstanceId(Duration)}}
     */
    Uuid clientInstanceId(Duration timeout);

    /**
     * 获取消费者的所有度量指标
     * @see KafkaConsumer#metrics()
     */
    Map<MetricName, ? extends Metric> metrics();

    /**
     * 获取指定主题的分区信息
     * @see KafkaConsumer#partitionsFor(String)
     */
    List<PartitionInfo> partitionsFor(String topic);

    /**
     * 在指定超时时间内获取指定主题的分区信息
     * @see KafkaConsumer#partitionsFor(String, Duration)
     */
    List<PartitionInfo> partitionsFor(String topic, Duration timeout);

    /**
     * 列出所有可用的主题及其分区信息
     * @see KafkaConsumer#listTopics()
     */
    Map<String, List<PartitionInfo>> listTopics();

    /**
     * 在指定超时时间内列出所有可用的主题及其分区信息
     * @see KafkaConsumer#listTopics(Duration)
     */
    Map<String, List<PartitionInfo>> listTopics(Duration timeout);

    /**
     * 获取当前已暂停的分区集合
     * @see KafkaConsumer#paused()
     */
    Set<TopicPartition> paused();

    /**
     * 暂停指定分区的消息获取
     * @see KafkaConsumer#pause(Collection)
     */
    void pause(Collection<TopicPartition> partitions);

    /**
     * 恢复指定分区的消息获取
     * @see KafkaConsumer#resume(Collection)
     */
    void resume(Collection<TopicPartition> partitions);

    /**
     * 查找大于等于指定时间戳的第一个记录的偏移量
     * @see KafkaConsumer#offsetsForTimes(Map)
     */
    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch);

    /**
     * 在指定超时时间内查找大于等于指定时间戳的第一个记录的偏移量
     * @see KafkaConsumer#offsetsForTimes(Map, Duration)
     */
    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout);

    /**
     * 获取指定分区的起始偏移量
     * @see KafkaConsumer#beginningOffsets(Collection)
     */
    Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions);

    /**
     * 在指定超时时间内获取指定分区的起始偏移量
     * @see KafkaConsumer#beginningOffsets(Collection, Duration)
     */
    Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout);

    /**
     * 获取指定分区的末尾偏移量
     * @see KafkaConsumer#endOffsets(Collection)
     */
    Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions);

    /**
     * 在指定超时时间内获取指定分区的末尾偏移量
     * @see KafkaConsumer#endOffsets(Collection, Duration)
     */
    Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout);

    /**
     * 获取指定分区的当前消费滞后量
     * @see KafkaConsumer#currentLag(TopicPartition)
     */
    OptionalLong currentLag(TopicPartition topicPartition);

    /**
     * 获取消费者组元数据
     * @see KafkaConsumer#groupMetadata()
     */
    ConsumerGroupMetadata groupMetadata();

    /**
     * 强制触发一次再平衡
     * @see KafkaConsumer#enforceRebalance()
     */
    void enforceRebalance();

    /**
     * 以指定原因强制触发一次再平衡
     * @see KafkaConsumer#enforceRebalance(String)
     */
    void enforceRebalance(final String reason);

    /**
     * 关闭消费者
     * @see KafkaConsumer#close()
     */
    void close();

    /**
     * 在指定超时时间内关闭消费者（已弃用）
     * @see KafkaConsumer#close(Duration)
     */
    @Deprecated
    void close(Duration timeout);

    /**
     * 使用指定选项关闭消费者
     * @see KafkaConsumer#close(CloseOptions)
     */
    void close(final CloseOptions option);

    /**
     * 唤醒消费者
     * @see KafkaConsumer#wakeup()
     */
    void wakeup();
}
