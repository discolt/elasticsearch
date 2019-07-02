/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.vpack.store;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteTransportException;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.vpack.store.StoreSettings.INDEX_STORE_REMOTE_SETTINGS;

/**
 * 存储分离引擎
 * 将_source字段转存至远程集群
 * 使本地集群成为纯索引集群
 */
public final class DetachStoredEngine extends InternalEngine {

    private final Client client;
    private final ThreadPool threadPool;
    private final String remoteCluster;
    private final String indexName;
    private final StoreSettings settings;
    private final AtomicLong queueUsedBytes;
    private final Queue<IndexRequest> queue;
    private static final String TYPE = "_doc";

    /**
     * 远程集群是否可服务
     */
    private volatile boolean remoteServable = true;

    /**
     * 是否开启分离存储
     */
    private volatile boolean enabled = true;

    /**
     * 获取_source字段序号，remove时提高性能
     */
    private volatile int indexOfSourceField = SOURCE_FILED_INDEX_UNASSIGNED;

    /**
     * findIndexOfSourceField: 待分配。
     */
    private static final int SOURCE_FILED_INDEX_UNASSIGNED = -1;

    /**
     * findIndexOfSourceField: 用户已关闭_source字段, 无需开启
     */
    private static final int SOURCE_FILED_NOT_FOUND = -2;

    public DetachStoredEngine(EngineConfig engineConfig, Client client, ThreadPool threadPool, StoreSettings settings) {
        super(engineConfig);
        this.client = client;
        this.settings = settings;
        this.threadPool = threadPool;
        this.indexName = engineConfig.getIndexSettings().getIndex().getName();
        this.remoteCluster = INDEX_STORE_REMOTE_SETTINGS.get(engineConfig.getIndexSettings().getSettings());
        this.queueUsedBytes = new AtomicLong(0);
        this.queue = new LinkedBlockingQueue();
        runConsumedTask();
    }

    @Override
    protected IndexingStrategy indexingStrategyForOperation(final Index index) throws IOException {
        if (hasBeenProcessedBefore(index)) {
            if (index.origin() == Operation.Origin.PRIMARY) {
                // 找到_source所在doc的序号，提高删除_source性能。
                if (indexOfSourceField == SOURCE_FILED_INDEX_UNASSIGNED) {
                    indexOfSourceField = findIndexOfSourceField(index);
                }
                // 用户未开启_source ， 转为本地写模式。
                if (indexOfSourceField == SOURCE_FILED_NOT_FOUND) {
                    enabled = false;
                    return super.indexingStrategyForOperation(index);
                }
                // 如远程集群不可服务，转为本地写模式。
                if (!remoteServable) {
                    return super.indexingStrategyForOperation(index);
                }
                // 真实的_source迁移处理
                // 将_source field放入队列，异步传输到远程集群
                // 当消费速度小于本地索引速度时，改为写本地，避免内存溢出
                if (queueUsedBytes.get() < settings.getQueueLimitSize().getBytes()) {
                    IndexRequest indexRequest = new IndexRequest(indexName, TYPE, index.id());
                    Map source = Collections.singletonMap("source", index.source().utf8ToString());
                    indexRequest.source(source);
                    index.docs().forEach(e -> e.getFields().remove(indexOfSourceField));
                    queueUsedBytes.addAndGet(indexRequest.source().ramBytesUsed());
                    queue.add(indexRequest);
                }
            }
        }
        return super.indexingStrategyForOperation(index);
    }

    /**
     * 查找source字段位置，只在第一个Index运行。
     *
     * @param index
     */
    private int findIndexOfSourceField(Index index) {
        List<IndexableField> fields = index.docs().get(0).getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name().equals("_source")) {
                return i;
            }
        }
        return SOURCE_FILED_NOT_FOUND;
    }

    private BulkRequestBuilder prepareBulk() {
        return client.getRemoteClusterClient(remoteCluster).prepareBulk();
    }

    /**
     * 后台消费队列
     */
    private void runConsumedTask() {
        threadPool.generic().submit(new AbstractRunnable() {
            @Override
            protected void doRun() {
                consuming();
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("remote store task error found: {}", e);
            }
        });
    }

    private void consuming() {
        while (enabled) {
            try {
                BulkRequestBuilder requestBuilder = prepareBulk();
                IndexRequest request;
                int sentBytesUsed = 0;
                while ((request = queue.poll()) != null) {
                    requestBuilder.add(request);
                    if ((sentBytesUsed += request.source().ramBytesUsed()) > settings.getBulkBufferSize().getBytes()) {
                        ensureOpen();
                        requestBuilder.execute(sentFailedHandler);
                        requestBuilder = prepareBulk();
                        queueUsedBytes.addAndGet(-sentBytesUsed);
                        sentBytesUsed = 0;
                    }
                }
                if (requestBuilder.numberOfActions() > 0) {
                    ensureOpen();
                    requestBuilder.execute(sentFailedHandler);
                    queueUsedBytes.addAndGet(-sentBytesUsed);
                }
            } catch (AlreadyClosedException e) {
                enabled = false;
                break;
            } catch (Exception e) {
                logger.error(e);
            }
            sleep(2);
        }
    }

    private ActionListener<BulkResponse> sentFailedHandler = new ActionListener<BulkResponse>() {
        // TODO failed的一般为全部
        @Override
        public void onResponse(BulkResponse bulkItemResponses) {
            if (bulkItemResponses.hasFailures()) {
                long c = Arrays.stream(bulkItemResponses.getItems()).filter(e -> e.isFailed()).count();
                logger.error("transfer source bulk total [{}] failed found [{}]", bulkItemResponses.getItems().length, c);
                //Arrays.stream(bulkItemResponses.getItems()).filter(e -> e.isFailed()).forEach(v -> logger.error(v.getFailureMessage()));
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (e instanceof ConnectTransportException || e instanceof RemoteTransportException) {
                makeRemoteServable(false);
                threadPool.generic().submit(resumeIfRecoverd);
            }
            logger.error(e);
        }
    };

    /**
     * 检查远程集群是否可服务
     */
    private Runnable resumeIfRecoverd = () -> {
        while (enabled) {
            if (remoteClusterServable()) {
                makeRemoteServable(true);
                break;
            } else {
                sleep(1);
            }
        }
    };

    private void makeRemoteServable(boolean servable) {
        if (servable) {
            logger.info("[DetachStoredEngine] Resume Remote-Storage-Strategy");
        } else {
            logger.warn("[DetachStoredEngine] Remote cluster unavailable, Change to Local-Storage-Strategy");
        }
        this.remoteServable = servable;
    }

    @Override
    public void close() throws IOException {
        enabled = false;
        super.close();
    }

    private void sleep(int second) {
        try {
            TimeUnit.SECONDS.sleep(second);
        } catch (InterruptedException e) {
            logger.error(e);
        }
    }

    private boolean remoteClusterServable() {
        try {
            Client remote = client.getRemoteClusterClient(remoteCluster);
            remote.admin().cluster().prepareClusterStats().get(TimeValue.timeValueSeconds(1));
        } catch (Exception e) {
            return false;
        }
        return true;
    }

}