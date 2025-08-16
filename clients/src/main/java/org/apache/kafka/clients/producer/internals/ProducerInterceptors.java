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
package org.apache.kafka.clients.producer.internals;


import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.RecordBatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;

/**
 * A container that holds the list {@link org.apache.kafka.clients.producer.ProducerInterceptor}
 * and wraps calls to the chain of custom interceptors.
 */
public class ProducerInterceptors<K, V> implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(ProducerInterceptors.class);
    private final List<Plugin<ProducerInterceptor<K, V>>> interceptorPlugins;

    public ProducerInterceptors(List<ProducerInterceptor<K, V>> interceptors, Metrics metrics) {
        this.interceptorPlugins = Plugin.wrapInstances(interceptors, metrics, ProducerConfig.INTERCEPTOR_CLASSES_CONFIG);
    }

    /**
     * 当客户端向KafkaProducer发送记录时调用此方法,在key和value序列化之前执行
     * 该方法会调用{@link ProducerInterceptor#onSend(ProducerRecord)}方法。第一个拦截器的onSend()返回的ProducerRecord
     * 会传递给第二个拦截器的onSend()方法,以此类推形成拦截器链。最后一个拦截器返回的记录将作为此方法的返回值。
     * 
     * 此方法不会抛出异常。任何拦截器方法抛出的异常都会被捕获并忽略。
     * 如果拦截器链中间的某个拦截器(通常会修改记录)抛出异常,
     * 链中的下一个拦截器将使用上一个没有抛出异常的拦截器返回的记录继续执行。
     *
     * @param record 来自客户端的记录
     * @return 要发送到topic/partition的生产者记录
     */
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        // 保存原始记录,用于拦截器链的处理
        ProducerRecord<K, V> interceptRecord = record;
        
        // 遍历所有注册的拦截器插件
        for (Plugin<ProducerInterceptor<K, V>> interceptorPlugin : this.interceptorPlugins) {
            try {
                // 调用当前拦截器的onSend方法,处理记录
                // 每个拦截器可以修改记录内容,修改后的记录会传给下一个拦截器
                interceptRecord = interceptorPlugin.get().onSend(interceptRecord);
            } catch (Exception e) {
                // 捕获拦截器可能抛出的异常
                // 不向上传播异常,只记录日志并继续调用其他拦截器
                if (record != null) {
                    // 如果原始记录不为空,记录主题和分区信息
                    log.warn("Error executing interceptor onSend callback for topic: {}, partition: {}", 
                            record.topic(), record.partition(), e);
                } else {
                    // 原始记录为空时的日志记录
                    log.warn("Error executing interceptor onSend callback", e);
                }
            }
        }
        
        // 返回经过所有拦截器处理后的记录
        return interceptRecord;
    }

    /**
     * 当记录发送到服务器并得到确认时,或在记录发送到服务器之前发送失败时调用此方法。
     * 此方法会为每个拦截器调用{@link ProducerInterceptor#onAcknowledgement(RecordMetadata, Exception, Headers)}方法。
     *
     * 此方法不会抛出异常。任何拦截器方法抛出的异常都会被捕获并忽略。
     *
     * @param metadata 已发送记录的元数据(即分区和偏移量)。
     *                如果发生错误,元数据将只包含有效的主题,可能还包含分区信息。
     * @param exception 处理此记录期间抛出的异常。如果没有发生错误则为null。
     * @param headers 已发送记录的头部信息
     */
    public void onAcknowledgement(RecordMetadata metadata, Exception exception, Headers headers) {
        for (Plugin<ProducerInterceptor<K, V>> interceptorPlugin : this.interceptorPlugins) {
            try {
                interceptorPlugin.get().onAcknowledgement(metadata, exception, headers);
            } catch (Exception e) {
                // do not propagate interceptor exceptions, just log
                log.warn("Error executing interceptor onAcknowledgement callback", e);
            }
        }
    }

    /**
     * 当记录在{@link ProducerInterceptor#onSend(ProducerRecord)}方法中发送失败时调用此方法。
     * 此方法会为每个拦截器调用{@link ProducerInterceptor#onAcknowledgement(RecordMetadata, Exception, Headers)}方法。
     *
     * @param record 来自客户端的记录
     * @param interceptTopicPartition 如果在分区分配后发生错误,则为记录的主题/分区;
     *                               interceptTopicPartition的主题部分与record中的相同
     * @param exception 处理此记录期间抛出的异常
     */
    public void onSendError(ProducerRecord<K, V> record, TopicPartition interceptTopicPartition, Exception exception) {
        // 遍历所有注册的拦截器插件
        for (Plugin<ProducerInterceptor<K, V>> interceptorPlugin : this.interceptorPlugins) {
            try {
                // 获取记录头部信息,如果记录为空则创建新的头部
                Headers headers = record != null ? record.headers() : new RecordHeaders();
                
                // 如果头部是可写的RecordHeaders类型,创建只读副本
                if (headers instanceof RecordHeaders && !((RecordHeaders) headers).isReadOnly()) {
                    // 创建头部副本以确保不会改变原始记录头部的状态
                    // 保持原始头部可写,因为客户端可能在重试前需要修改它们
                    RecordHeaders recordHeaders = (RecordHeaders) headers;
                    headers = new RecordHeaders(recordHeaders);
                    ((RecordHeaders) headers).setReadOnly();
                }

                // 根据记录和主题分区的状态调用不同的onAcknowledgement处理逻辑
                if (record == null && interceptTopicPartition == null) {
                    // 如果记录和主题分区都为空,传递null元数据
                    interceptorPlugin.get().onAcknowledgement(null, exception, headers);
                } else {
                    // 如果主题分区为空但记录不为空,从记录中提取主题分区信息
                    if (interceptTopicPartition == null) {
                        interceptTopicPartition = extractTopicPartition(record);
                    }
                    // 创建包含错误信息的记录元数据(-1表示无效值)
                    interceptorPlugin.get().onAcknowledgement(new RecordMetadata(interceptTopicPartition, -1, -1,
                                    RecordBatch.NO_TIMESTAMP, -1, -1), exception, headers);
                }
            } catch (Exception e) {
                // 捕获拦截器异常但不向上传播,只记录警告日志
                log.warn("Error executing interceptor onAcknowledgement callback", e);
            }
        }
    }

    public static <K, V> TopicPartition extractTopicPartition(ProducerRecord<K, V> record) {
        return new TopicPartition(record.topic(), record.partition() == null ? RecordMetadata.UNKNOWN_PARTITION : record.partition());
    }

    /**
     * 关闭容器中的所有拦截器。
     * 
     * 该方法会遍历所有注册的拦截器插件,依次调用它们的close()方法进行关闭。
     * 如果某个拦截器关闭过程中抛出异常,会被捕获并记录错误日志,但不会影响其他拦截器的关闭。
     * 这样可以确保所有拦截器都有机会执行清理工作,即使部分拦截器关闭失败。
     */
    @Override
    public void close() {
        for (Plugin<ProducerInterceptor<K, V>> interceptorPlugin : this.interceptorPlugins) {
            try {
                interceptorPlugin.close();
            } catch (Exception e) {
                log.error("Failed to close producer interceptor ", e);
            }
        }
    }
}
