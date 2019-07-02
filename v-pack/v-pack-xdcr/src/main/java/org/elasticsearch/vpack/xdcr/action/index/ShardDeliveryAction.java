/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.vpack.xdcr.action.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.*;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.SingleShardOperationRequestBuilder;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardNotStartedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.vpack.xdcr.action.index.bulk.BulkShardOperationsAction;
import org.elasticsearch.vpack.xdcr.action.index.bulk.BulkShardOperationsRequest;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.vpack.xdcr.action.index.PutIndexAction.IMD_CUSTOM_XDCR;
import static org.elasticsearch.vpack.xdcr.action.index.PutIndexAction.IMD_CUSTOM_XDCR_KEY_LEADER_INDEX_UUID;

/**
 * 传递分片数据
 */
public class ShardDeliveryAction extends Action<ShardDeliveryAction.Request, ShardDeliveryAction.Response, ShardDeliveryAction.RequestBuilder> {

    private static final Logger logger = LogManager.getLogger(ShardDeliveryAction.class);
    public static final ShardDeliveryAction INSTANCE = new ShardDeliveryAction();
    public static final String NAME = "indices:internal/vpack/xdcr/index/shard_delivery";

    private ShardDeliveryAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends SingleShardRequest<Request> {

        private long fromSeqNo;
        private int maxOperationCount;
        private ShardId shardId;
        private TimeValue pollTimeout = TimeValue.timeValueSeconds(5);
        private ByteSizeValue maxBatchSize = new ByteSizeValue(10, ByteSizeUnit.MB);
        private String repository;
        private long relativeStartNanos;

        public Request(ShardId shardId, String repository) {
            super(shardId.getIndexName());
            this.shardId = shardId;
            this.repository = repository;
        }

        Request() {
        }

        public ShardId getShard() {
            return shardId;
        }

        public long getFromSeqNo() {
            return fromSeqNo;
        }

        public void setFromSeqNo(long fromSeqNo) {
            this.fromSeqNo = fromSeqNo;
        }

        public int getMaxOperationCount() {
            return maxOperationCount;
        }

        public void setMaxOperationCount(int maxOperationCount) {
            this.maxOperationCount = maxOperationCount;
        }

        public ByteSizeValue getMaxBatchSize() {
            return maxBatchSize;
        }

        public void setMaxBatchSize(ByteSizeValue maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
        }

        public TimeValue getPollTimeout() {
            return pollTimeout;
        }

        public void setPollTimeout(final TimeValue pollTimeout) {
            this.pollTimeout = Objects.requireNonNull(pollTimeout, "pollTimeout");
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (fromSeqNo < 0) {
                validationException = addValidationError("fromSeqNo [" + fromSeqNo + "] cannot be lower than 0", validationException);
            }
            if (maxOperationCount < 0) {
                validationException = addValidationError("maxOperationCount [" + maxOperationCount + "] cannot be lower than 0", validationException);
            }
            if (maxBatchSize.compareTo(ByteSizeValue.ZERO) <= 0) {
                validationException = addValidationError("maxBatchSize [" + maxBatchSize.getStringRep() + "] must be larger than 0", validationException);
            }
            if (repository == null) {
                validationException = addValidationError("repository must specify", validationException);
            }
            return validationException;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            fromSeqNo = in.readVLong();
            maxOperationCount = in.readVInt();
            shardId = ShardId.readShardId(in);
            pollTimeout = in.readTimeValue();
            maxBatchSize = new ByteSizeValue(in);
            repository = in.readString();
            relativeStartNanos = System.nanoTime();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(fromSeqNo);
            out.writeVInt(maxOperationCount);
            shardId.writeTo(out);
            out.writeTimeValue(pollTimeout);
            maxBatchSize.writeTo(out);
            out.writeString(repository);
        }


        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Request request = (Request) o;
            return fromSeqNo == request.fromSeqNo &&
                    maxOperationCount == request.maxOperationCount &&
                    Objects.equals(shardId, request.shardId) &&
                    Objects.equals(pollTimeout, request.pollTimeout) &&
                    maxBatchSize.equals(request.maxBatchSize) &&
                    repository.equals(request.repository);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fromSeqNo, maxOperationCount, shardId, pollTimeout, maxBatchSize, repository);
        }

        @Override
        public String toString() {
            return "Request{" +
                    "fromSeqNo=" + fromSeqNo +
                    ", maxOperationCount=" + maxOperationCount +
                    ", shardId=" + shardId +
                    ", pollTimeout=" + pollTimeout +
                    ", maxBatchSize=" + maxBatchSize.getStringRep() +
                    ", repository=" + repository +
                    '}';
        }

    }

    public static final class Response extends ActionResponse {

        private long mappingVersion;

        private long settingsVersion;

        private long globalCheckpoint;

        private long maxSeqNo;

        private long maxSeqNoOfUpdatesOrDeletes;

        private long tookInMillis;

        public long getTookInMillis() {
            return tookInMillis;
        }

        Response() {
        }

        Response(
                final long mappingVersion,
                final long settingsVersion,
                final long globalCheckpoint,
                final long maxSeqNo,
                final long maxSeqNoOfUpdatesOrDeletes,
                final long tookInMillis) {

            this.mappingVersion = mappingVersion;
            this.settingsVersion = settingsVersion;
            this.globalCheckpoint = globalCheckpoint;
            this.maxSeqNo = maxSeqNo;
            this.maxSeqNoOfUpdatesOrDeletes = maxSeqNoOfUpdatesOrDeletes;
            this.tookInMillis = tookInMillis;
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            super.readFrom(in);
            mappingVersion = in.readVLong();
            settingsVersion = in.readVLong();
            globalCheckpoint = in.readZLong();
            maxSeqNo = in.readZLong();
            maxSeqNoOfUpdatesOrDeletes = in.readZLong();
            tookInMillis = in.readVLong();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(mappingVersion);
            out.writeVLong(settingsVersion);
            out.writeZLong(globalCheckpoint);
            out.writeZLong(maxSeqNo);
            out.writeZLong(maxSeqNoOfUpdatesOrDeletes);
            out.writeVLong(tookInMillis);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Response that = (Response) o;
            return mappingVersion == that.mappingVersion &&
                    settingsVersion == that.settingsVersion &&
                    globalCheckpoint == that.globalCheckpoint &&
                    maxSeqNo == that.maxSeqNo &&
                    maxSeqNoOfUpdatesOrDeletes == that.maxSeqNoOfUpdatesOrDeletes &&
                    tookInMillis == that.tookInMillis;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    mappingVersion,
                    settingsVersion,
                    globalCheckpoint,
                    maxSeqNo,
                    maxSeqNoOfUpdatesOrDeletes,
                    tookInMillis);
        }
    }

    static class RequestBuilder extends SingleShardOperationRequestBuilder<Request, Response, RequestBuilder> {

        RequestBuilder(ElasticsearchClient client, Action<Request, Response, RequestBuilder> action) {
            super(client, action, new Request());
        }
    }

    public static class TransportAction extends TransportSingleShardAction<Request, Response> {

        private final Client client;
        private final IndicesService indicesService;

        @Inject
        public TransportAction(Settings settings,
                               Client client,
                               ThreadPool threadPool,
                               ClusterService clusterService,
                               TransportService transportService,
                               ActionFilters actionFilters,
                               IndexNameExpressionResolver indexNameExpressionResolver,
                               IndicesService indicesService) {
            super(settings, NAME, threadPool, clusterService, transportService, actionFilters,
                    indexNameExpressionResolver, Request::new, ThreadPool.Names.SEARCH);
            this.client = client;
            this.indicesService = indicesService;
        }

        @Override
        protected Response shardOperation(Request request, ShardId shardId) throws IOException {
            final IndexService indexService = indicesService.indexServiceSafe(request.getShard().getIndex());
            final IndexShard indexShard = indexService.getShard(request.getShard().id());
            final SeqNoStats seqNoStats = indexShard.seqNoStats();
            final IndexMetaData indexMetaData = clusterService.state().metaData().index(shardId.getIndex());
            final long mappingVersion = indexMetaData.getMappingVersion();
            final long settingsVersion = indexMetaData.getSettingsVersion();
            Client remoteClient = client.getRemoteClusterClient(request.repository);
            deliveryOperations(
                    indexShard,
                    seqNoStats.getGlobalCheckpoint(),
                    request.getFromSeqNo(),
                    request.getMaxOperationCount(),
                    request.getMaxBatchSize(),
                    remoteClient);
            // must capture after after snapshotting operations to ensure this MUS is at least the highest MUS notNull any notNull these operations.
            final long maxSeqNoOfUpdatesOrDeletes = indexShard.getMaxSeqNoOfUpdatesOrDeletes();
            return getResponse(
                    mappingVersion,
                    settingsVersion,
                    seqNoStats,
                    maxSeqNoOfUpdatesOrDeletes,
                    request.relativeStartNanos);
        }

        @Override
        protected void asyncShardOperation(
                final Request request,
                final ShardId shardId,
                final ActionListener<Response> listener) throws IOException {
            final IndexService indexService = indicesService.indexServiceSafe(request.getShard().getIndex());
            final IndexShard indexShard = indexService.getShard(request.getShard().id());
            final SeqNoStats seqNoStats = indexShard.seqNoStats();

            if (request.getFromSeqNo() > seqNoStats.getGlobalCheckpoint()) {
                logger.trace(
                        "{} waiting for global checkpoint advancement from [{}] to [{}]",
                        shardId,
                        seqNoStats.getGlobalCheckpoint(),
                        request.getFromSeqNo());
                indexShard.addGlobalCheckpointListener(
                        request.getFromSeqNo(),
                        (g, e) -> {
                            if (g != UNASSIGNED_SEQ_NO) {
                                assert request.getFromSeqNo() <= g
                                        : shardId + " only advanced to [" + g + "] while waiting for [" + request.getFromSeqNo() + "]";
                                globalCheckpointAdvanced(shardId, g, request, listener);
                            } else {
                                assert e != null;
                                globalCheckpointAdvancementFailure(shardId, e, request, listener, indexShard);
                            }
                        },
                        request.getPollTimeout());
            } else {
                super.asyncShardOperation(request, shardId, listener);
            }
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            ActionListener<Response> wrappedListener = ActionListener.wrap(listener::onResponse, e -> {
                Throwable cause = ExceptionsHelper.unwrapCause(e);
                if (cause instanceof IllegalStateException && cause.getMessage().contains("Not all operations between from_seqno [")) {
                    String message = "Operations are no longer available for replicating. Maybe increase the retention setting [" +
                            IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey() + "]?";
                    listener.onFailure(new ElasticsearchException(message, e));
                } else {
                    listener.onFailure(e);
                }
            });
            super.doExecute(task, request, wrappedListener);
        }

        private void globalCheckpointAdvanced(
                final ShardId shardId,
                final long globalCheckpoint,
                final Request request,
                final ActionListener<Response> listener) {
            logger.trace("{} global checkpoint advanced to [{}] after waiting for [{}]", shardId, globalCheckpoint, request.getFromSeqNo());
            try {
                super.asyncShardOperation(request, shardId, listener);
            } catch (final IOException caught) {
                listener.onFailure(caught);
            }
        }

        private void globalCheckpointAdvancementFailure(
                final ShardId shardId,
                final Exception e,
                final Request request,
                final ActionListener<Response> listener,
                final IndexShard indexShard) {
            logger.trace(
                    () -> new ParameterizedMessage(
                            "{} exception waiting for global checkpoint advancement to [{}]", shardId, request.getFromSeqNo()),
                    e);
            if (e instanceof TimeoutException) {
                try {
                    final IndexMetaData indexMetaData = clusterService.state().metaData().index(shardId.getIndex());
                    final long mappingVersion = indexMetaData.getMappingVersion();
                    final long settingsVersion = indexMetaData.getSettingsVersion();
                    final SeqNoStats latestSeqNoStats = indexShard.seqNoStats();
                    final long maxSeqNoOfUpdatesOrDeletes = indexShard.getMaxSeqNoOfUpdatesOrDeletes();
                    listener.onResponse(
                            getResponse(
                                    mappingVersion,
                                    settingsVersion,
                                    latestSeqNoStats,
                                    maxSeqNoOfUpdatesOrDeletes,
                                    request.relativeStartNanos));
                } catch (final Exception caught) {
                    caught.addSuppressed(e);
                    listener.onFailure(caught);
                }
            } else {
                listener.onFailure(e);
            }
        }

        @Override
        protected boolean resolveIndex(Request request) {
            return false;
        }

        @Override
        protected ShardsIterator shards(ClusterState state, InternalRequest request) {
            return state
                    .routingTable()
                    .shardRoutingTable(request.concreteIndex(), request.request().getShard().id())
                    .activeInitializingShardsRandomIt();
        }

        @Override
        protected Response newResponse() {
            return new Response();
        }

    }

    /**
     * 传递分片数据
     *
     * @param indexShard        the shard
     * @param globalCheckpoint  the global checkpoint
     * @param fromSeqNo         the starting sequence number
     * @param maxOperationCount the maximum number notNull operations
     * @param maxBatchSize      the maximum batch size
     * @param remote            the remote client
     * @throws IOException if an I/O exception occurs reading the operations
     */
    static void deliveryOperations(
            final IndexShard indexShard,
            final long globalCheckpoint,
            final long fromSeqNo,
            final int maxOperationCount,
            final ByteSizeValue maxBatchSize,
            final Client remote) throws IOException {
        if (indexShard.state() != IndexShardState.STARTED) {
            throw new IndexShardNotStartedException(indexShard.shardId(), indexShard.state());
        }
        if (fromSeqNo > globalCheckpoint) {
            throw new IllegalStateException(
                    "not exposing operations from [" + fromSeqNo + "] greater than the global checkpoint [" + globalCheckpoint + "]");
        }

        int seenBytes = 0;
        // - 1 is needed, because toSeqNo is inclusive
        long toSeqNo = Math.min(globalCheckpoint, (fromSeqNo + maxOperationCount) - 1);
        assert fromSeqNo <= toSeqNo : "invalid range from_seqno[" + fromSeqNo + "] > to_seqno[" + toSeqNo + "]";
        final List<Translog.Operation> operations = new ArrayList<>();
        final ShardId remoteShardId = remoteShardId(remote, indexShard.shardId());
        try (Translog.Snapshot snapshot = indexShard.newChangesSnapshot("xdcr", fromSeqNo, toSeqNo, true)) {
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                operations.add(op);
                seenBytes += op.estimateSize();
                if (seenBytes > maxBatchSize.getBytes()) {
                    logger.debug("{} delivery:{}-{}", indexShard.shardId(), operations.get(0).seqNo(), operations.get(operations.size() - 1).seqNo());
                    BulkShardOperationsRequest bulkRequest = new BulkShardOperationsRequest(remoteShardId, operations, indexShard.getMaxSeqNoOfUpdatesOrDeletes());
                    remote.execute(BulkShardOperationsAction.INSTANCE, bulkRequest).actionGet();
                    operations.clear();
                    seenBytes = 0;
                }
            }
            if (!operations.isEmpty()) {
                BulkShardOperationsRequest bulkRequest = new BulkShardOperationsRequest(remoteShardId, operations, indexShard.getMaxSeqNoOfUpdatesOrDeletes());
                remote.execute(BulkShardOperationsAction.INSTANCE, bulkRequest).actionGet();
            }
        }
    }

    static Response getResponse(
            final long mappingVersion,
            final long settingsVersion,
            final SeqNoStats seqNoStats,
            final long maxSeqNoOfUpdates,
            long relativeStartNanos) {
        long tookInNanos = System.nanoTime() - relativeStartNanos;
        long tookInMillis = TimeUnit.NANOSECONDS.toMillis(tookInNanos);
        return new Response(
                mappingVersion,
                settingsVersion,
                seqNoStats.getGlobalCheckpoint(),
                seqNoStats.getMaxSeqNo(),
                maxSeqNoOfUpdates,
                tookInMillis);
    }

    static ShardId remoteShardId(Client remote, ShardId shardId) {
        String index = shardId.getIndexName();
        String currentIndexUUID = shardId.getIndex().getUUID();
        String remoteIndexUUID = Optional.ofNullable(remote.admin().cluster().prepareState().get()
                .getState().metaData().index(index).getCustomData(IMD_CUSTOM_XDCR).get(IMD_CUSTOM_XDCR_KEY_LEADER_INDEX_UUID))
                .orElseThrow(() -> new IllegalStateException("remote index custom uuid not found " + index + ":" + currentIndexUUID));
        if (!currentIndexUUID.equals(remoteIndexUUID)) {
            throw new IllegalStateException("index uuid no match , current " + currentIndexUUID + " remote:" + remoteIndexUUID);
        }
        return Optional.ofNullable(remote.admin().cluster().prepareState().get().getState())
                .filter(clusterState -> clusterState.metaData().index(index).getState().equals(IndexMetaData.State.OPEN))
                .map(clusterState -> clusterState.routingTable().index(index).shard(shardId.id()).primaryShard().shardId())
                .orElseThrow(() -> new NoShardAvailableActionException(shardId, "remote shard not available"));
    }


}
