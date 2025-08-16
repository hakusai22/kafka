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

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MetadataSnapshot;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.FencedLeaderEpochException;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TransactionAbortedException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 后台线程,负责向Kafka集群发送生产请求。该线程会定期发送元数据请求来更新集群视图,
 * 然后将生产请求发送到相应的节点。
 */
public class Sender implements Runnable {

    /**
     * 日志记录器
     */
    private final Logger log;

    /**
     * 每个节点的连接状态
     */
    private final KafkaClient client;

    /**
     * 用于批量处理记录的累加器
     */
    private final RecordAccumulator accumulator;

    /**
     * 客户端的元数据
     */
    private final ProducerMetadata metadata;

    /**
     * 标识生产者是否需要保证消息在broker上的顺序
     */
    private final boolean guaranteeMessageOrder;

    /**
     * 尝试发送到服务器的最大请求大小
     */
    private final int maxRequestSize;

    /**
     * 向服务器请求的确认数
     */
    private final short acks;

    /**
     * 失败请求重试次数上限
     */
    private final int retries;

    /**
     * 用于获取时间的时钟实例
     */
    private final Time time;

    /**
     * 标识发送线程是否仍在运行
     */
    private volatile boolean running;

    /**
     * 标识调用者是否要忽略所有未发送/在途消息并强制关闭
     */
    private volatile boolean forceClose;

    /**
     * 指标统计
     */
    private final SenderMetrics sensors;

    /**
     * 等待服务器响应请求的最大时间
     */
    private final int requestTimeoutMs;

    /**
     * 重试失败请求前等待的最大时间
     */
    private final long retryBackoffMs;

    /**
     * 事务相关的所有状态,特别是生产者ID、生产者epoch和序列号
     */
    private final TransactionManager transactionManager;

    /**
     * 按创建时间排序的每个分区批次队列,用于跟踪在途批次
     */
    private final Map<TopicPartition, List<ProducerBatch>> inFlightBatches;

    public Sender(LogContext logContext,
                  KafkaClient client,
                  ProducerMetadata metadata,
                  RecordAccumulator accumulator,
                  boolean guaranteeMessageOrder,
                  int maxRequestSize,
                  short acks,
                  int retries,
                  SenderMetricsRegistry metricsRegistry,
                  Time time,
                  int requestTimeoutMs,
                  long retryBackoffMs,
                  TransactionManager transactionManager) {
        this.log = logContext.logger(Sender.class);
        this.client = client;
        this.accumulator = accumulator;
        this.metadata = metadata;
        this.guaranteeMessageOrder = guaranteeMessageOrder;
        this.maxRequestSize = maxRequestSize;
        this.running = true;
        this.acks = acks;
        this.retries = retries;
        this.time = time;
        this.sensors = new SenderMetrics(metricsRegistry, metadata, client, time);
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
        this.transactionManager = transactionManager;
        this.inFlightBatches = new HashMap<>();
    }

    public List<ProducerBatch> inFlightBatches(TopicPartition tp) {
        return inFlightBatches.containsKey(tp) ? inFlightBatches.get(tp) : new ArrayList<>();
    }

    private void maybeRemoveFromInflightBatches(ProducerBatch batch) {
        List<ProducerBatch> batches = inFlightBatches.get(batch.topicPartition);
        if (batches != null) {
            batches.remove(batch);
            if (batches.isEmpty()) {
                inFlightBatches.remove(batch.topicPartition);
            }
        }
    }

    private void maybeRemoveAndDeallocateBatch(ProducerBatch batch) {
        maybeRemoveFromInflightBatches(batch);
        this.accumulator.deallocate(batch);
    }

    /**
     *  Get the in-flight batches that has reached delivery timeout.
     */
    private List<ProducerBatch> getExpiredInflightBatches(long now) {
        List<ProducerBatch> expiredBatches = new ArrayList<>();

        for (Iterator<Map.Entry<TopicPartition, List<ProducerBatch>>> batchIt = inFlightBatches.entrySet().iterator(); batchIt.hasNext();) {
            Map.Entry<TopicPartition, List<ProducerBatch>> entry = batchIt.next();
            List<ProducerBatch> partitionInFlightBatches = entry.getValue();
            if (partitionInFlightBatches != null) {
                Iterator<ProducerBatch> iter = partitionInFlightBatches.iterator();
                while (iter.hasNext()) {
                    ProducerBatch batch = iter.next();
                    if (batch.hasReachedDeliveryTimeout(accumulator.getDeliveryTimeoutMs(), now)) {
                        iter.remove();
                        // expireBatches is called in Sender.sendProducerData, before client.poll.
                        // The !batch.isDone() invariant should always hold. An IllegalStateException
                        // exception will be thrown if the invariant is violated.
                        if (!batch.isDone()) {
                            expiredBatches.add(batch);
                        } else {
                            throw new IllegalStateException(batch.topicPartition + " batch created at " +
                                batch.createdMs + " gets unexpected final state " + batch.finalState());
                        }
                    } else {
                        accumulator.maybeUpdateNextBatchExpiryTime(batch);
                        break;
                    }
                }
                if (partitionInFlightBatches.isEmpty()) {
                    batchIt.remove();
                }
            }
        }
        return expiredBatches;
    }

    private void addToInflightBatches(List<ProducerBatch> batches) {
        for (ProducerBatch batch : batches) {
            List<ProducerBatch> inflightBatchList = inFlightBatches.computeIfAbsent(batch.topicPartition,
                k -> new ArrayList<>());
            inflightBatchList.add(batch);
        }
    }

    public void addToInflightBatches(Map<Integer, List<ProducerBatch>> batches) {
        for (List<ProducerBatch> batchList : batches.values()) {
            addToInflightBatches(batchList);
        }
    }

    private boolean hasPendingTransactionalRequests() {
        return transactionManager != null && transactionManager.hasPendingRequests() && transactionManager.hasOngoingTransaction();
    }

    /**
     * 发送者线程的主循环
     * 
     * 该循环会不断执行以下操作:
     * 1. 从RecordAccumulator获取准备好的消息批次
     * 2. 将这些批次通过网络发送到Kafka集群
     * 3. 处理响应和错误
     * 4. 管理事务状态(如果启用了事务)
     */
    @Override
    public void run() {
        log.debug("Starting Kafka producer I/O thread.");

        // 主循环,直到调用close方法才会退出
        while (running) {
            try {
                runOnce();
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        log.debug("Beginning shutdown of Kafka producer I/O thread, sending remaining records.");

        // 虽然已停止接受新的请求,但事务管理器、累加器中可能还有请求,或者正在等待确认
        // 等待这些请求完成
        while (!forceClose && ((this.accumulator.hasUndrained() || this.client.inFlightRequestCount() > 0) || hasPendingTransactionalRequests())) {
            try {
                runOnce();
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        // 如果有未完成的事务提交或中止没有通过事务管理器的队列,则中止事务
        while (!forceClose && transactionManager != null && transactionManager.hasOngoingTransaction()) {
            if (!transactionManager.isCompleting()) {
                log.info("Aborting incomplete transaction due to shutdown");
                try {
                    // 事务管理器在中止时可能会抛出错误,捕获这些错误
                    // 以免影响其他关闭逻辑
                    transactionManager.beginAbort();
                } catch (Exception e) {
                    log.error("Error in kafka producer I/O thread while aborting transaction when during closing: ", e);
                    // 如果事务管理器处于错误状态,则强制关闭
                    forceClose = true;
                }
            }
            try {
                runOnce();
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        if (forceClose) {
            // 需要使所有未完成的事务请求和批次失败,并唤醒等待future的线程
            if (transactionManager != null) {
                log.debug("Aborting incomplete transactional requests due to forced shutdown");
                transactionManager.close();
            }
            log.debug("Aborting incomplete batches due to forced shutdown");
            this.accumulator.abortIncompleteBatches();
        }
        try {
            this.client.close();
        } catch (Exception e) {
            log.error("Failed to close network client", e);
        }

        log.debug("Shutdown of Kafka producer I/O thread has completed.");
    }

    /**
     * 执行一次发送迭代
     * 
     * 主要流程:
     * 1. 如果启用了事务,先处理事务相关逻辑:
     *    - 解析序列号
     *    - 处理事务错误
     *    - 更新生产者ID
     *    - 发送事务控制请求
     * 
     * 2. 执行常规消息发送:
     *    - 获取准备好的消息批次
     *    - 将批次发送到对应的broker节点
     *    - 处理发送响应
     *    - 执行网络轮询
     *
     */
    void runOnce() {
        // 如果存在事务管理器,则需要先处理事务相关的逻辑
        if (transactionManager != null) {
            try {
                // 尝试解析序列号
                transactionManager.maybeResolveSequences();

                // 获取最近一次的错误
                RuntimeException lastError = transactionManager.lastError();

                // 如果事务管理器处于致命错误状态,则不继续发送
                if (transactionManager.hasFatalError()) {
                    // 如果有错误,可能需要中止批次
                    if (lastError != null)
                        maybeAbortBatches(lastError);
                    // 执行一次网络轮询,然后返回
                    client.poll(retryBackoffMs, time.milliseconds());
                    return;
                }

                // 如果有可中止的错误且需要处理授权错误,则直接返回
                if (transactionManager.hasAbortableError() && shouldHandleAuthorizationError(lastError)) {
                    return;
                }

                // 检查是否需要新的生产者ID
                // 如果需要,将会入队一个InitProducerId请求
                transactionManager.bumpIdempotentEpochAndResetIdIfNeeded();

                // 尝试发送和轮询事务请求
                // 如果有事务请求被处理,则返回
                if (maybeSendAndPollTransactionalRequest()) {
                    return;
                }
            } catch (AuthenticationException e) {
                // 捕获认证异常,记录日志并通知事务管理器
                log.trace("Authentication exception while processing transactional request", e);
                transactionManager.authenticationFailed(e);
            }
        }

        // 获取当前时间戳
        long currentTimeMs = time.milliseconds();
        // 发送生产者数据,获取下一次轮询超时时间
        long pollTimeout = sendProducerData(currentTimeMs);
        // 执行一次网络轮询
        client.poll(pollTimeout, currentTimeMs);
    }

    // We handle {@code TransactionalIdAuthorizationException} and {@code ClusterAuthorizationException} by first
    // failing the inflight requests, then transition the state to UNINITIALIZED so that the user doesn't need to
    // instantiate the producer again.
    private boolean shouldHandleAuthorizationError(RuntimeException exception) {
        if (exception instanceof TransactionalIdAuthorizationException ||
                        exception instanceof ClusterAuthorizationException) {
            transactionManager.failPendingRequests(new AuthenticationException(exception));
            maybeAbortBatches(exception);
            transactionManager.transitionToUninitialized(exception);
            return true;
        }
        return false;
    }

    private long sendProducerData(long now) {
        MetadataSnapshot metadataSnapshot = metadata.fetchMetadataSnapshot();
        // get the list of partitions with data ready to send
        RecordAccumulator.ReadyCheckResult result = this.accumulator.ready(metadataSnapshot, now);

        // if there are any partitions whose leaders are not known yet, force metadata update
        if (!result.unknownLeaderTopics.isEmpty()) {
            // The set of topics with unknown leader contains topics with leader election pending as well as
            // topics which may have expired. Add the topic again to metadata to ensure it is included
            // and request metadata update, since there are messages to send to the topic.
            for (String topic : result.unknownLeaderTopics)
                this.metadata.add(topic, now);

            log.debug("Requesting metadata update due to unknown leader topics from the batched records: {}",
                result.unknownLeaderTopics);
            this.metadata.requestUpdate(false);
        }

        // remove any nodes we aren't ready to send to
        Iterator<Node> iter = result.readyNodes.iterator();
        long notReadyTimeout = Long.MAX_VALUE;
        while (iter.hasNext()) {
            Node node = iter.next();
            if (!this.client.ready(node, now)) {
                // Update just the readyTimeMs of the latency stats, so that it moves forward
                // every time the batch is ready (then the difference between readyTimeMs and
                // drainTimeMs would represent how long data is waiting for the node).
                this.accumulator.updateNodeLatencyStats(node.id(), now, false);
                iter.remove();
                notReadyTimeout = Math.min(notReadyTimeout, this.client.pollDelayMs(node, now));
            } else {
                // Update both readyTimeMs and drainTimeMs, this would "reset" the node
                // latency.
                this.accumulator.updateNodeLatencyStats(node.id(), now, true);
            }
        }

        // create produce requests
        Map<Integer, List<ProducerBatch>> batches = this.accumulator.drain(metadataSnapshot, result.readyNodes, this.maxRequestSize, now);
        addToInflightBatches(batches);
        if (guaranteeMessageOrder) {
            // Mute all the partitions drained
            for (List<ProducerBatch> batchList : batches.values()) {
                for (ProducerBatch batch : batchList)
                    this.accumulator.mutePartition(batch.topicPartition);
            }
        }

        accumulator.resetNextBatchExpiryTime();
        List<ProducerBatch> expiredInflightBatches = getExpiredInflightBatches(now);
        List<ProducerBatch> expiredBatches = this.accumulator.expiredBatches(now);
        expiredBatches.addAll(expiredInflightBatches);

        // Reset the producer id if an expired batch has previously been sent to the broker. Also update the metrics
        // for expired batches. see the documentation of @TransactionState.resetIdempotentProducerId to understand why
        // we need to reset the producer id here.
        if (!expiredBatches.isEmpty())
            log.trace("Expired {} batches in accumulator", expiredBatches.size());
        for (ProducerBatch expiredBatch : expiredBatches) {
            String errorMessage = "Expiring " + expiredBatch.recordCount + " record(s) for " + expiredBatch.topicPartition
                + ":" + (now - expiredBatch.createdMs) + " ms has passed since batch creation";
            failBatch(expiredBatch, new TimeoutException(errorMessage), false);
            if (transactionManager != null && expiredBatch.inRetry()) {
                // This ensures that no new batches are drained until the current in flight batches are fully resolved.
                transactionManager.markSequenceUnresolved(expiredBatch);
            }
        }
        sensors.updateProduceRequestMetrics(batches);

        // If we have any nodes that are ready to send + have sendable data, poll with 0 timeout so this can immediately
        // loop and try sending more data. Otherwise, the timeout will be the smaller value between next batch expiry
        // time, and the delay time for checking data availability. Note that the nodes may have data that isn't yet
        // sendable due to lingering, backing off, etc. This specifically does not include nodes with sendable data
        // that aren't ready to send since they would cause busy looping.
        long pollTimeout = Math.min(result.nextReadyCheckDelayMs, notReadyTimeout);
        pollTimeout = Math.min(pollTimeout, this.accumulator.nextExpiryTimeMs() - now);
        pollTimeout = Math.max(pollTimeout, 0);
        if (!result.readyNodes.isEmpty()) {
            log.trace("Nodes with data ready to send: {}", result.readyNodes);
            // if some partitions are already ready to be sent, the select time would be 0;
            // otherwise if some partition already has some data accumulated but not ready yet,
            // the select time will be the time difference between now and its linger expiry time;
            // otherwise the select time will be the time difference between now and the metadata expiry time;
            pollTimeout = 0;
        }
        sendProduceRequests(batches, now);
        return pollTimeout;
    }

    /**
     * Returns true if a transactional request is sent or polled, or if a FindCoordinator request is enqueued
     */
    private boolean maybeSendAndPollTransactionalRequest() {
        if (transactionManager.hasInFlightRequest()) {
            // as long as there are outstanding transactional requests, we simply wait for them to return
            client.poll(retryBackoffMs, time.milliseconds());
            return true;
        }

        if (transactionManager.hasAbortableError()) {
            accumulator.abortUndrainedBatches(transactionManager.lastError());
        } else if (transactionManager.isAborting()) {
            accumulator.abortUndrainedBatches(new TransactionAbortedException());
        }

        TransactionManager.TxnRequestHandler nextRequestHandler = transactionManager.nextRequest(accumulator.hasIncomplete());
        if (nextRequestHandler == null)
            return false;

        AbstractRequest.Builder<?> requestBuilder = nextRequestHandler.requestBuilder();
        Node targetNode = null;
        try {
            FindCoordinatorRequest.CoordinatorType coordinatorType = nextRequestHandler.coordinatorType();
            targetNode = coordinatorType != null ?
                    transactionManager.coordinator(coordinatorType) :
                    client.leastLoadedNode(time.milliseconds()).node();
            if (targetNode != null) {
                if (!awaitNodeReady(targetNode, coordinatorType)) {
                    log.trace("Target node {} not ready within request timeout, will retry when node is ready.", targetNode);
                    maybeFindCoordinatorAndRetry(nextRequestHandler);
                    return true;
                }
            } else if (coordinatorType != null) {
                log.trace("Coordinator not known for {}, will retry {} after finding coordinator.", coordinatorType, requestBuilder.apiKey());
                maybeFindCoordinatorAndRetry(nextRequestHandler);
                return true;
            } else {
                log.trace("No nodes available to send requests, will poll and retry when until a node is ready.");
                transactionManager.retry(nextRequestHandler);
                client.poll(retryBackoffMs, time.milliseconds());
                return true;
            }

            if (nextRequestHandler.isRetry())
                time.sleep(nextRequestHandler.retryBackoffMs());

            long currentTimeMs = time.milliseconds();
            ClientRequest clientRequest = client.newClientRequest(targetNode.idString(), requestBuilder, currentTimeMs,
                true, requestTimeoutMs, nextRequestHandler);
            log.debug("Sending transactional request {} to node {} with correlation ID {}", requestBuilder, targetNode, clientRequest.correlationId());
            client.send(clientRequest, currentTimeMs);
            transactionManager.setInFlightCorrelationId(clientRequest.correlationId());
            client.poll(retryBackoffMs, time.milliseconds());
            return true;
        } catch (IOException e) {
            log.debug("Disconnect from {} while trying to send request {}. Going " +
                    "to back off and retry.", targetNode, requestBuilder, e);
            // We break here so that we pick up the FindCoordinator request immediately.
            maybeFindCoordinatorAndRetry(nextRequestHandler);
            return true;
        }
    }

    private void maybeFindCoordinatorAndRetry(TransactionManager.TxnRequestHandler nextRequestHandler) {
        if (nextRequestHandler.needsCoordinator()) {
            transactionManager.lookupCoordinator(nextRequestHandler);
        } else {
            // For non-coordinator requests, sleep here to prevent a tight loop when no node is available
            time.sleep(retryBackoffMs);
            metadata.requestUpdate(false);
        }

        transactionManager.retry(nextRequestHandler);
    }

    private void maybeAbortBatches(RuntimeException exception) {
        if (accumulator.hasIncomplete()) {
            log.error("Aborting producer batches due to fatal error", exception);
            accumulator.abortBatches(exception);
        }
    }

    /**
     * Start closing the sender (won't actually complete until all data is sent out)
     */
    public void initiateClose() {
        // Ensure accumulator is closed first to guarantee that no more appends are accepted after
        // breaking from the sender loop. Otherwise, we may miss some callbacks when shutting down.
        this.accumulator.close();
        this.running = false;
        this.wakeup();
    }

    /**
     * Closes the sender without sending out any pending messages.
     */
    public void forceClose() {
        this.forceClose = true;
        initiateClose();
    }

    public boolean isRunning() {
        return running;
    }

    private boolean awaitNodeReady(Node node, FindCoordinatorRequest.CoordinatorType coordinatorType) throws IOException {
        if (NetworkClientUtils.awaitReady(client, node, time, requestTimeoutMs)) {
            if (coordinatorType == FindCoordinatorRequest.CoordinatorType.TRANSACTION) {
                // Indicate to the transaction manager that the coordinator is ready, allowing it to check ApiVersions
                // This allows us to bump transactional epochs even if the coordinator is temporarily unavailable at
                // the time when the abortable error is handled
                transactionManager.handleCoordinatorReady();
            }
            return true;
        }
        return false;
    }

    /**
     * Handle a produce response
     */
    private void handleProduceResponse(ClientResponse response, Map<TopicPartition, ProducerBatch> batches, Map<Uuid, String> topicNames, long now) {
        RequestHeader requestHeader = response.requestHeader();
        int correlationId = requestHeader.correlationId();
        if (response.wasTimedOut()) {
            log.trace("Cancelled request with header {} due to the last request to node {} timed out",
                requestHeader, response.destination());
            for (ProducerBatch batch : batches.values())
                completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.REQUEST_TIMED_OUT, String.format("Disconnected from node %s due to timeout", response.destination())),
                        correlationId, now, null);
        } else if (response.wasDisconnected()) {
            log.trace("Cancelled request with header {} due to node {} being disconnected",
                requestHeader, response.destination());
            for (ProducerBatch batch : batches.values())
                completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NETWORK_EXCEPTION, String.format("Disconnected from node %s", response.destination())),
                        correlationId, now, null);
        } else if (response.versionMismatch() != null) {
            log.warn("Cancelled request {} due to a version mismatch with node {}",
                    response, response.destination(), response.versionMismatch());
            for (ProducerBatch batch : batches.values())
                completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.UNSUPPORTED_VERSION), correlationId, now, null);
        } else {
            log.trace("Received produce response from node {} with correlation id {}", response.destination(), correlationId);
            // if we have a response, parse it
            if (response.hasResponse()) {
                // Sender should exercise PartitionProduceResponse rather than ProduceResponse.PartitionResponse
                // https://issues.apache.org/jira/browse/KAFKA-10696
                ProduceResponse produceResponse = (ProduceResponse) response.responseBody();
                // This will be set by completeBatch.
                Map<TopicPartition, Metadata.LeaderIdAndEpoch> partitionsWithUpdatedLeaderInfo = new HashMap<>();
                produceResponse.data().responses().forEach(r -> r.partitionResponses().forEach(p -> {
                    ProduceResponse.PartitionResponse partResp = new ProduceResponse.PartitionResponse(
                            Errors.forCode(p.errorCode()),
                            p.baseOffset(),
                            p.logAppendTimeMs(),
                            p.logStartOffset(),
                            p.recordErrors()
                                .stream()
                                .map(e -> new ProduceResponse.RecordError(e.batchIndex(), e.batchIndexErrorMessage()))
                                .collect(Collectors.toList()),
                            p.errorMessage(),
                            p.currentLeader());

                    // Version 13 drop topic name and add support to topic id.
                    // We need to find batch based on topic id and partition index only as
                    // topic name in the response will be empty.
                    // For older versions, topic id is zero, and we will find the batch based on the topic name.
                    TopicPartition tp = (!r.topicId().equals(Uuid.ZERO_UUID) && topicNames.containsKey(r.topicId())) ?
                            new TopicPartition(topicNames.get(r.topicId()), p.index()) :
                            new TopicPartition(r.name(), p.index());

                    ProducerBatch batch = batches.get(tp);
                    if (batch == null) {
                        throw new IllegalStateException("Can't find batch created for topic id " + r.topicId() +
                                " topic name " + r.name() + " partition " + p.index() + " using " + topicNames);
                    }
                    completeBatch(batch, partResp, correlationId, now, partitionsWithUpdatedLeaderInfo);
                }));

                if (!partitionsWithUpdatedLeaderInfo.isEmpty()) {
                    List<Node> leaderNodes = produceResponse.data().nodeEndpoints().stream()
                        .map(e -> new Node(e.nodeId(), e.host(), e.port(), e.rack()))
                        .filter(e -> !e.equals(Node.noNode()))
                        .collect(
                            Collectors.toList());
                    Set<TopicPartition> updatedPartitions = metadata.updatePartitionLeadership(partitionsWithUpdatedLeaderInfo, leaderNodes);
                    if (log.isTraceEnabled()) {
                        updatedPartitions.forEach(
                            part -> log.debug("For {} leader was updated.", part)
                        );
                    }
                }

                this.sensors.recordLatency(response.destination(), response.requestLatencyMs());
            } else {
                // this is the acks = 0 case, just complete all requests
                for (ProducerBatch batch : batches.values()) {
                    completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NONE), correlationId, now, null);
                }
            }
        }
    }

    /**
     * Complete or retry the given batch of records.
     *
     * @param batch The record batch
     * @param response The produce response
     * @param correlationId The correlation id for the request
     * @param now The current POSIX timestamp in milliseconds
     * @param partitionsWithUpdatedLeaderInfo This will be populated with partitions that have updated leader info.
     */
    private void completeBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response, long correlationId,
                               long now, Map<TopicPartition, Metadata.LeaderIdAndEpoch> partitionsWithUpdatedLeaderInfo) {
        Errors error = response.error;

        if (error == Errors.MESSAGE_TOO_LARGE && batch.recordCount > 1 && !batch.isDone() &&
                (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 || batch.isCompressed())) {
            // If the batch is too large, we split the batch and send the split batches again. We do not decrement
            // the retry attempts in this case.
            log.warn(
                "Got error produce response in correlation id {} on topic-partition {}, splitting and retrying ({} attempts left). Error: {}",
                correlationId,
                batch.topicPartition,
                this.retries - batch.attempts(),
                formatErrMsg(response));
            if (transactionManager != null)
                transactionManager.removeInFlightBatch(batch);
            this.accumulator.splitAndReenqueue(batch);
            maybeRemoveAndDeallocateBatch(batch);
            this.sensors.recordBatchSplit();
        } else if (error != Errors.NONE) {
            if (canRetry(batch, response, now)) {
                log.warn(
                    "Got error produce response with correlation id {} on topic-partition {}, retrying ({} attempts left). Error: {}",
                    correlationId,
                    batch.topicPartition,
                    this.retries - batch.attempts() - 1,
                    formatErrMsg(response));
                reenqueueBatch(batch, now);
            } else if (error == Errors.DUPLICATE_SEQUENCE_NUMBER) {
                // If we have received a duplicate sequence error, it means that the sequence number has advanced beyond
                // the sequence of the current batch, and we haven't retained batch metadata on the broker to return
                // the correct offset and timestamp.
                //
                // The only thing we can do is to return success to the user and not return a valid offset and timestamp.
                completeBatch(batch, response);
            } else {
                // tell the user the result of their request. We only adjust sequence numbers if the batch didn't exhaust
                // its retries -- if it did, we don't know whether the sequence number was accepted or not, and
                // thus it is not safe to reassign the sequence.
                failBatch(batch, response, batch.attempts() < this.retries);
            }
            if (error.exception() instanceof InvalidMetadataException) {
                if (error.exception() instanceof UnknownTopicOrPartitionException) {
                    log.warn("Received unknown topic or partition error in produce request on partition {}. The " +
                            "topic-partition may not exist or the user may not have Describe access to it",
                        batch.topicPartition);
                } else {
                    log.warn("Received invalid metadata error in produce request on partition {} due to {} Going " +
                            "to request metadata update now", batch.topicPartition, error.exception(response.errorMessage).toString());
                }
                if (error.exception() instanceof NotLeaderOrFollowerException || error.exception() instanceof FencedLeaderEpochException) {
                    log.debug("For {}, received error {}, with leaderIdAndEpoch {}", batch.topicPartition, error, response.currentLeader);
                    if (partitionsWithUpdatedLeaderInfo != null
                        && (response.currentLeader.leaderId() != -1 && response.currentLeader.leaderEpoch() != -1)) {
                        partitionsWithUpdatedLeaderInfo.put(batch.topicPartition, new Metadata.LeaderIdAndEpoch(
                            Optional.of(response.currentLeader.leaderId()), Optional.of(response.currentLeader.leaderEpoch())));
                    }
                }
                metadata.requestUpdate(false);
            }
        } else {
            completeBatch(batch, response);
        }

        // Unmute the completed partition.
        if (guaranteeMessageOrder)
            this.accumulator.unmutePartition(batch.topicPartition);
    }

    /**
     * Format the error from a {@link ProduceResponse.PartitionResponse} in a user-friendly string
     * e.g "NETWORK_EXCEPTION. Error Message: Disconnected from node 0"
     */
    private String formatErrMsg(ProduceResponse.PartitionResponse response) {
        String errorMessageSuffix = (response.errorMessage == null || response.errorMessage.isEmpty()) ?
                "" : String.format(". Error Message: %s", response.errorMessage);
        return String.format("%s%s", response.error, errorMessageSuffix);
    }

    private void reenqueueBatch(ProducerBatch batch, long currentTimeMs) {
        this.accumulator.reenqueue(batch, currentTimeMs);
        maybeRemoveFromInflightBatches(batch);
        this.sensors.recordRetries(batch.topicPartition.topic(), batch.recordCount);
    }

    private void completeBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response) {
        if (transactionManager != null) {
            transactionManager.handleCompletedBatch(batch, response);
        }

        if (batch.complete(response.baseOffset, response.logAppendTime)) {
            maybeRemoveAndDeallocateBatch(batch);
        }
    }

    private void failBatch(ProducerBatch batch,
                           ProduceResponse.PartitionResponse response,
                           boolean adjustSequenceNumbers) {
        final RuntimeException topLevelException;
        if (response.error == Errors.TOPIC_AUTHORIZATION_FAILED)
            topLevelException = new TopicAuthorizationException(Collections.singleton(batch.topicPartition.topic()));
        else if (response.error == Errors.CLUSTER_AUTHORIZATION_FAILED)
            topLevelException = new ClusterAuthorizationException("The producer is not authorized to do idempotent sends");
        else
            topLevelException = response.error.exception(response.errorMessage);

        if (response.recordErrors == null || response.recordErrors.isEmpty()) {
            failBatch(batch, topLevelException, adjustSequenceNumbers);
        } else {
            Map<Integer, RuntimeException> recordErrorMap = new HashMap<>(response.recordErrors.size());
            for (ProduceResponse.RecordError recordError : response.recordErrors) {
                // The API leaves us with some awkwardness interpreting the errors in the response.
                // We cannot differentiate between different error cases (such as INVALID_TIMESTAMP)
                // from the single error code at the partition level, so instead we use INVALID_RECORD
                // for all failed records and rely on the message to distinguish the cases.
                final String errorMessage;
                if (recordError.message != null) {
                    errorMessage = recordError.message;
                } else if (response.errorMessage != null) {
                    errorMessage = response.errorMessage;
                } else {
                    errorMessage = response.error.message();
                }

                // If the batch contained only a single record error, then we can unambiguously
                // use the exception type corresponding to the partition-level error code.
                if (response.recordErrors.size() == 1) {
                    recordErrorMap.put(recordError.batchIndex, response.error.exception(errorMessage));
                } else {
                    recordErrorMap.put(recordError.batchIndex, new InvalidRecordException(errorMessage));
                }
            }

            Function<Integer, RuntimeException> recordExceptions = batchIndex -> {
                RuntimeException exception = recordErrorMap.get(batchIndex);
                if (exception != null) {
                    return exception;
                } else {
                    // If the response contains record errors, then the records which failed validation
                    // will be present in the response. To avoid confusion for the remaining records, we
                    // return a generic exception.
                    return new KafkaException("Failed to append record because it was part of a batch " +
                        "which had one more more invalid records");
                }
            };

            failBatch(batch, topLevelException, recordExceptions, adjustSequenceNumbers);
        }
    }

    private void failBatch(
        ProducerBatch batch,
        RuntimeException topLevelException,
        boolean adjustSequenceNumbers
    ) {
        failBatch(batch, topLevelException, batchIndex -> topLevelException, adjustSequenceNumbers);
    }

    private void failBatch(
        ProducerBatch batch,
        RuntimeException topLevelException,
        Function<Integer, RuntimeException> recordExceptions,
        boolean adjustSequenceNumbers
    ) {
        this.sensors.recordErrors(batch.topicPartition.topic(), batch.recordCount);

        if (batch.completeExceptionally(topLevelException, recordExceptions)) {
            if (transactionManager != null) {
                try {
                    // This call can throw an exception in the rare case that there's an invalid state transition
                    // attempted. Catch these so as not to interfere with the rest of the logic.
                    transactionManager.handleFailedBatch(batch, topLevelException, adjustSequenceNumbers);
                } catch (Exception e) {
                    log.debug("Encountered error when transaction manager was handling a failed batch", e);
                }
            }
            maybeRemoveAndDeallocateBatch(batch);
        }
    }

    /**
     * We can retry a send if the error is transient and the number of attempts taken is fewer than the maximum allowed.
     * We can also retry OutOfOrderSequence exceptions for future batches, since if the first batch has failed, the
     * future batches are certain to fail with an OutOfOrderSequence exception.
     */
    private boolean canRetry(ProducerBatch batch, ProduceResponse.PartitionResponse response, long now) {
        return !batch.hasReachedDeliveryTimeout(accumulator.getDeliveryTimeoutMs(), now) &&
            batch.attempts() < this.retries &&
            !batch.isDone() &&
            (transactionManager == null ?
                    response.error.exception() instanceof RetriableException :
                    transactionManager.canRetry(response, batch));
    }

    /**
     * Transfer the record batches into a list of produce requests on a per-node basis
     */
    private void sendProduceRequests(Map<Integer, List<ProducerBatch>> collated, long now) {
        for (Map.Entry<Integer, List<ProducerBatch>> entry : collated.entrySet())
            sendProduceRequest(now, entry.getKey(), acks, requestTimeoutMs, entry.getValue());
    }

    /**
     * Create a produce request from the given record batches
     */
    private void sendProduceRequest(long now, int destination, short acks, int timeout, List<ProducerBatch> batches) {
        if (batches.isEmpty())
            return;

        final Map<TopicPartition, ProducerBatch> recordsByPartition = new HashMap<>(batches.size());
        Map<String, Uuid> topicIds = topicIdsForBatches(batches);

        ProduceRequestData.TopicProduceDataCollection tpd = new ProduceRequestData.TopicProduceDataCollection();
        for (ProducerBatch batch : batches) {
            TopicPartition tp = batch.topicPartition;
            MemoryRecords records = batch.records();
            Uuid topicId = topicIds.get(tp.topic());
            ProduceRequestData.TopicProduceData tpData = tpd.find(tp.topic(), topicId);

            if (tpData == null) {
                tpData = new ProduceRequestData.TopicProduceData()
                        .setTopicId(topicId).setName(tp.topic());
                tpd.add(tpData);
            }

            tpData.partitionData().add(new ProduceRequestData.PartitionProduceData()
                    .setIndex(tp.partition())
                    .setRecords(records));
            recordsByPartition.put(tp, batch);
        }

        String transactionalId = null;
        boolean useTransactionV1Version = false;
        if (transactionManager != null && transactionManager.isTransactional()) {
            transactionalId = transactionManager.transactionalId();
            useTransactionV1Version = !transactionManager.isTransactionV2Enabled();
        }

        ProduceRequest.Builder requestBuilder = ProduceRequest.builder(
                new ProduceRequestData()
                        .setAcks(acks)
                        .setTimeoutMs(timeout)
                        .setTransactionalId(transactionalId)
                        .setTopicData(tpd),
                useTransactionV1Version
        );
        // Fetch topic names from metadata outside callback as topic ids may change during the callback
        // for example if topic was recreated.
        Map<Uuid, String> topicNames = metadata.topicNames();

        RequestCompletionHandler callback = response -> handleProduceResponse(response, recordsByPartition, topicNames, time.milliseconds());

        String nodeId = Integer.toString(destination);
        ClientRequest clientRequest = client.newClientRequest(nodeId, requestBuilder, now, acks != 0,
                requestTimeoutMs, callback);
        client.send(clientRequest, now);
        log.trace("Sent produce request to {}: {}", nodeId, requestBuilder);
    }

    private Map<String, Uuid> topicIdsForBatches(List<ProducerBatch> batches) {
        return batches.stream()
                .collect(Collectors.toMap(
                        b -> b.topicPartition.topic(),
                        b -> metadata.topicIds().getOrDefault(b.topicPartition.topic(), Uuid.ZERO_UUID),
                        (existing, replacement) -> replacement)
                );
    }

    /**
     * Wake up the selector associated with this send thread
     */
    public void wakeup() {
        this.client.wakeup();
    }

    public static Sensor throttleTimeSensor(SenderMetricsRegistry metrics) {
        Sensor produceThrottleTimeSensor = metrics.sensor("produce-throttle-time");
        produceThrottleTimeSensor.add(metrics.produceThrottleTimeAvg, new Avg());
        produceThrottleTimeSensor.add(metrics.produceThrottleTimeMax, new Max());
        return produceThrottleTimeSensor;
    }

    /**
     * A collection of sensors for the sender
     */
    private static class SenderMetrics {
        public final Sensor retrySensor;
        public final Sensor errorSensor;
        public final Sensor queueTimeSensor;
        public final Sensor requestTimeSensor;
        public final Sensor recordsPerRequestSensor;
        public final Sensor batchSizeSensor;
        public final Sensor compressionRateSensor;
        public final Sensor maxRecordSizeSensor;
        public final Sensor batchSplitSensor;
        private final SenderMetricsRegistry metrics;
        private final Time time;

        public SenderMetrics(SenderMetricsRegistry metrics, Metadata metadata, KafkaClient client, Time time) {
            this.metrics = metrics;
            this.time = time;

            this.batchSizeSensor = metrics.sensor("batch-size");
            this.batchSizeSensor.add(metrics.batchSizeAvg, new Avg());
            this.batchSizeSensor.add(metrics.batchSizeMax, new Max());

            this.compressionRateSensor = metrics.sensor("compression-rate");
            this.compressionRateSensor.add(metrics.compressionRateAvg, new Avg());

            this.queueTimeSensor = metrics.sensor("queue-time");
            this.queueTimeSensor.add(metrics.recordQueueTimeAvg, new Avg());
            this.queueTimeSensor.add(metrics.recordQueueTimeMax, new Max());

            this.requestTimeSensor = metrics.sensor("request-time");
            this.requestTimeSensor.add(metrics.requestLatencyAvg, new Avg());
            this.requestTimeSensor.add(metrics.requestLatencyMax, new Max());

            this.recordsPerRequestSensor = metrics.sensor("records-per-request");
            this.recordsPerRequestSensor.add(new Meter(metrics.recordSendRate, metrics.recordSendTotal));
            this.recordsPerRequestSensor.add(metrics.recordsPerRequestAvg, new Avg());

            this.retrySensor = metrics.sensor("record-retries");
            this.retrySensor.add(new Meter(metrics.recordRetryRate, metrics.recordRetryTotal));

            this.errorSensor = metrics.sensor("errors");
            this.errorSensor.add(new Meter(metrics.recordErrorRate, metrics.recordErrorTotal));

            this.maxRecordSizeSensor = metrics.sensor("record-size");
            this.maxRecordSizeSensor.add(metrics.recordSizeMax, new Max());
            this.maxRecordSizeSensor.add(metrics.recordSizeAvg, new Avg());

            this.metrics.addMetric(metrics.requestsInFlight, (config, now) -> client.inFlightRequestCount());
            this.metrics.addMetric(metrics.metadataAge,
                (config, now) -> (now - metadata.lastSuccessfulUpdate()) / 1000.0);

            this.batchSplitSensor = metrics.sensor("batch-split-rate");
            this.batchSplitSensor.add(new Meter(metrics.batchSplitRate, metrics.batchSplitTotal));
        }

        private void maybeRegisterTopicMetrics(String topic) {
            // if one sensor of the metrics has been registered for the topic,
            // then all other sensors should have been registered; and vice versa
            String topicRecordsCountName = "topic." + topic + ".records-per-batch";
            Sensor topicRecordCount = this.metrics.getSensor(topicRecordsCountName);
            if (topicRecordCount == null) {
                Map<String, String> metricTags = Collections.singletonMap("topic", topic);

                topicRecordCount = this.metrics.sensor(topicRecordsCountName);
                MetricName rateMetricName = this.metrics.topicRecordSendRate(metricTags);
                MetricName totalMetricName = this.metrics.topicRecordSendTotal(metricTags);
                topicRecordCount.add(new Meter(rateMetricName, totalMetricName));

                String topicByteRateName = "topic." + topic + ".bytes";
                Sensor topicByteRate = this.metrics.sensor(topicByteRateName);
                rateMetricName = this.metrics.topicByteRate(metricTags);
                totalMetricName = this.metrics.topicByteTotal(metricTags);
                topicByteRate.add(new Meter(rateMetricName, totalMetricName));

                String topicCompressionRateName = "topic." + topic + ".compression-rate";
                Sensor topicCompressionRate = this.metrics.sensor(topicCompressionRateName);
                MetricName m = this.metrics.topicCompressionRate(metricTags);
                topicCompressionRate.add(m, new Avg());

                String topicRetryName = "topic." + topic + ".record-retries";
                Sensor topicRetrySensor = this.metrics.sensor(topicRetryName);
                rateMetricName = this.metrics.topicRecordRetryRate(metricTags);
                totalMetricName = this.metrics.topicRecordRetryTotal(metricTags);
                topicRetrySensor.add(new Meter(rateMetricName, totalMetricName));

                String topicErrorName = "topic." + topic + ".record-errors";
                Sensor topicErrorSensor = this.metrics.sensor(topicErrorName);
                rateMetricName = this.metrics.topicRecordErrorRate(metricTags);
                totalMetricName = this.metrics.topicRecordErrorTotal(metricTags);
                topicErrorSensor.add(new Meter(rateMetricName, totalMetricName));
            }
        }

        public void updateProduceRequestMetrics(Map<Integer, List<ProducerBatch>> batches) {
            long now = time.milliseconds();
            for (List<ProducerBatch> nodeBatch : batches.values()) {
                int records = 0;
                for (ProducerBatch batch : nodeBatch) {
                    // register all per-topic metrics at once
                    String topic = batch.topicPartition.topic();
                    maybeRegisterTopicMetrics(topic);

                    // per-topic record send rate
                    String topicRecordsCountName = "topic." + topic + ".records-per-batch";
                    Sensor topicRecordCount = Objects.requireNonNull(this.metrics.getSensor(topicRecordsCountName));
                    topicRecordCount.record(batch.recordCount);

                    // per-topic bytes send rate
                    String topicByteRateName = "topic." + topic + ".bytes";
                    Sensor topicByteRate = Objects.requireNonNull(this.metrics.getSensor(topicByteRateName));
                    topicByteRate.record(batch.estimatedSizeInBytes());

                    // per-topic compression rate
                    String topicCompressionRateName = "topic." + topic + ".compression-rate";
                    Sensor topicCompressionRate = Objects.requireNonNull(this.metrics.getSensor(topicCompressionRateName));
                    topicCompressionRate.record(batch.compressionRatio());

                    // global metrics
                    this.batchSizeSensor.record(batch.estimatedSizeInBytes(), now);
                    this.queueTimeSensor.record(batch.queueTimeMs(), now);
                    this.compressionRateSensor.record(batch.compressionRatio());
                    this.maxRecordSizeSensor.record(batch.maxRecordSize, now);
                    records += batch.recordCount;
                }
                this.recordsPerRequestSensor.record(records, now);
            }
        }

        public void recordRetries(String topic, int count) {
            long now = time.milliseconds();
            this.retrySensor.record(count, now);
            String topicRetryName = "topic." + topic + ".record-retries";
            Sensor topicRetrySensor = this.metrics.getSensor(topicRetryName);
            if (topicRetrySensor != null)
                topicRetrySensor.record(count, now);
        }

        public void recordErrors(String topic, int count) {
            long now = time.milliseconds();
            this.errorSensor.record(count, now);
            String topicErrorName = "topic." + topic + ".record-errors";
            Sensor topicErrorSensor = this.metrics.getSensor(topicErrorName);
            if (topicErrorSensor != null)
                topicErrorSensor.record(count, now);
        }

        public void recordLatency(String node, long latency) {
            long now = time.milliseconds();
            this.requestTimeSensor.record(latency, now);
            if (!node.isEmpty()) {
                String nodeTimeName = "node-" + node + ".latency";
                Sensor nodeRequestTime = this.metrics.getSensor(nodeTimeName);
                if (nodeRequestTime != null)
                    nodeRequestTime.record(latency, now);
            }
        }

        void recordBatchSplit() {
            this.batchSplitSensor.record();
        }
    }

    public static class SenderThread extends KafkaThread {

        public SenderThread(final String name, Runnable runnable, boolean daemon) {
            super(name, runnable, daemon);
        }
    }
}
