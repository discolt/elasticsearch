/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.vpack.xdcr.cluster;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.elasticsearch.vpack.xdcr.core.TaskService;
import org.elasticsearch.vpack.xdcr.metadata.Feed;
import org.elasticsearch.vpack.xdcr.metadata.FeedInProgress;
import org.elasticsearch.vpack.xdcr.metadata.IndexId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

/**
 * This service runs on data and master nodes and controls currently snapshotted shards on these nodes. It is responsible for
 * starting and stopping shard level snapshots
 */
public class PushingShardsService extends AbstractLifecycleComponent implements ClusterStateListener, IndexEventListener {

    public static final String UPDATE_SNAPSHOT_STATUS_ACTION_NAME_V6 = "internal:cluster/startRecovery/update_status_v6";
    public static final String UPDATE_SNAPSHOT_STATUS_ACTION_NAME = "internal:cluster/startRecovery/update_status";


    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final TaskService taskService;
    private final TransportService transportService;
    private final Lock shutdownLock = new ReentrantLock();
    private final Condition shutdownCondition = shutdownLock.newCondition();
    private volatile Map<Feed, PushingShards> shardPushings = emptyMap();
    private final PushingStateExecutor pushingStateExecutor = new PushingStateExecutor();
    private UpdatePushingStatusAction updatePushingStatusHandler;

    @Inject
    public PushingShardsService(Settings settings, ClusterService clusterService, ThreadPool threadPool,
                                TransportService transportService, IndicesService indicesService,
                                TaskService taskService, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings);
        this.indicesService = indicesService;
        this.taskService = taskService;
        this.transportService = transportService;
        this.clusterService = clusterService;
        if (DiscoveryNode.isDataNode(settings)) {
            clusterService.addListener(this);
        }

        // The constructor of UpdateSnapshotStatusAction will register itself to the TransportService.
        this.updatePushingStatusHandler = new UpdatePushingStatusAction(settings, UPDATE_SNAPSHOT_STATUS_ACTION_NAME,
                transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver);

        if (DiscoveryNode.isMasterNode(settings)) {
            // This needs to run only on nodes that can become masters
            transportService.registerRequestHandler(UPDATE_SNAPSHOT_STATUS_ACTION_NAME_V6, UpdatePushingStatusRequestV6::new, ThreadPool.Names.SAME, new UpdatePushingStateRequestHandlerV6());
        }

    }

    @Override
    protected void doStart() {
        assert this.updatePushingStatusHandler != null;
        assert transportService.getRequestHandler(UPDATE_SNAPSHOT_STATUS_ACTION_NAME) != null;
        if (DiscoveryNode.isMasterNode(settings)) {
            assert transportService.getRequestHandler(UPDATE_SNAPSHOT_STATUS_ACTION_NAME_V6) != null;
        }
    }

    @Override
    protected void doStop() {
        shutdownLock.lock();
        try {
            while (!shardPushings.isEmpty() && shutdownCondition.await(5, TimeUnit.SECONDS)) {
                // Wait for at most 5 second for locally running pushing to finish
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            shutdownLock.unlock();
        }

    }

    @Override
    protected void doClose() {
        clusterService.removeListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        try {
            FeedInProgress prev = event.previousState().custom(FeedInProgress.TYPE);
            FeedInProgress curr = event.state().custom(FeedInProgress.TYPE);

            if ((prev == null && curr != null) || (prev != null && prev.equals(curr) == false)) {
                processIndexShardPushings(event);
            }
            String masterNodeId = event.state().nodes().getMasterNodeId();
            if (masterNodeId != null && masterNodeId.equals(event.previousState().nodes().getMasterNodeId()) == false) {
                syncShardStatsOnNewMaster(event);
            }

        } catch (Exception e) {
            logger.warn("Failed to update index state ", e);
        }
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        // abort any snapshots occurring on the soon-to-be closed shard
        Map<Feed, PushingShards> snapshotShardsMap = shardPushings;
        for (Map.Entry<Feed, PushingShards> snapshotShards : snapshotShardsMap.entrySet()) {
            Map<ShardId, IndexShardPushingStatus> shards = snapshotShards.getValue().shards;
            if (shards.containsKey(shardId)) {
                logger.debug("[{}] shard closing, abort snapshotting for index [{}]", shardId, snapshotShards.getKey().getFeedId());
                shards.get(shardId).abort();
            }
        }
    }

    /**
     * Returns status of shards that are snapshotted on the node and belong to the given index
     * <p>
     * This method is executed on data node
     * </p>
     *
     * @param feed index
     * @return map of shard id to index status
     */
    public Map<ShardId, IndexShardPushingStatus> currentPushingShards(Feed feed) {
        PushingShards pushingShards = shardPushings.get(feed);
        if (pushingShards == null) {
            return null;
        } else {
            return pushingShards.shards;
        }
    }

    /**
     * Checks if any new shards should be push on this node
     *
     * @param event cluster state changed event
     */
    private void processIndexShardPushings(ClusterChangedEvent event) {
        FeedInProgress feedInProgress = event.state().custom(FeedInProgress.TYPE);
        Map<Feed, PushingShards> survivors = new HashMap<>();
        for (Map.Entry<Feed, PushingShards> entry : shardPushings.entrySet()) {
            final Feed feed = entry.getKey();
            if (feedInProgress != null && feedInProgress.feed(feed) != null) {
                Map<ShardId, IndexShardPushingStatus> started = new HashMap<>();
                PushingShards pushingShards = entry.getValue();
                pushingShards.shards.forEach((k, v) -> {
                    if (feedInProgress.feed(feed).shard(k).state().equals(FeedInProgress.State.STARTED)) {
                        started.put(k, v);
                    }
                });
                PushingShards startedShards = new PushingShards(started);
                survivors.put(entry.getKey(), startedShards);
            } else {
                for (IndexShardPushingStatus pushingStatus : entry.getValue().shards.values()) {
                    if (pushingStatus.stage() == IndexShardPushingStatus.Stage.INIT || pushingStatus.stage() == IndexShardPushingStatus.Stage.STARTED) {
                        for (ShardId shardId : entry.getValue().shards.keySet()) {
                            taskService.cancel(feed.getRepository(), shardId.getIndexName(), shardId.id());
                        }
                        pushingStatus.abort();
                    }
                }
            }
        }

        // For now we will be mostly dealing with a single index at a time but might have multiple simultaneously running
        // snapshots in the future
        Map<Feed, Map<ShardId, IndexShardPushingStatus>> newSnapshots = new HashMap<>();
        // Now go through all snapshots and createOrUpdate existing or createOrUpdate missing
        final String localNodeId = event.state().nodes().getLocalNodeId();
        final DiscoveryNode masterNode = event.state().nodes().getMasterNode();
        final Map<Feed, Map<String, IndexId>> snapshotIndices = new HashMap<>();
        if (feedInProgress != null) {
            for (FeedInProgress.Entry entry : feedInProgress.entries()) {
                snapshotIndices.put(entry.feed(),
                        entry.indices().stream().collect(Collectors.toMap(IndexId::getName, Function.identity())));
                if (entry.state() == FeedInProgress.State.STARTED) {
                    Map<ShardId, IndexShardPushingStatus> startedShards = new HashMap<>();
                    PushingShards pushingShards = shardPushings.get(entry.feed());
                    for (ObjectObjectCursor<ShardId, FeedInProgress.ShardPushingStatus> shard : entry.shards()) {
                        // Add all new shards to start processing on
                        if (localNodeId.equals(shard.value.nodeId())) {
                            if (shard.value.state() == FeedInProgress.State.INIT && (pushingShards == null || !pushingShards.shards.containsKey(shard.key))) {
                                logger.trace("[{}] - Adding shard to the queue", shard.key);
                                startedShards.put(shard.key, new IndexShardPushingStatus());
                            }
                        }
                    }
                    if (!startedShards.isEmpty()) {
                        newSnapshots.put(entry.feed(), startedShards);
                        if (pushingShards != null) {
                            // We already saw this index but we need to add more started shards
                            Map<ShardId, IndexShardPushingStatus> shards = new HashMap<>();
                            // Put all shards that were already running on this node
                            shards.putAll(pushingShards.shards);
                            // Put all newly started shards
                            shards.putAll(startedShards);
                            survivors.put(entry.feed(), new PushingShards(unmodifiableMap(shards)));
                        } else {
                            // Brand new index that we haven't seen before
                            survivors.put(entry.feed(), new PushingShards(unmodifiableMap(startedShards)));
                        }
                    }
                } else if (entry.state() == FeedInProgress.State.ABORTED) {
                    // Abort all running shards for this index
                    PushingShards pushingShards = shardPushings.get(entry.feed());
                    if (pushingShards != null) {
                        for (ObjectObjectCursor<ShardId, FeedInProgress.ShardPushingStatus> shard : entry.shards()) {
                            IndexShardPushingStatus snapshotStatus = pushingShards.shards.get(shard.key);
                            if (snapshotStatus != null) {
                                switch (snapshotStatus.stage()) {
                                    case INIT:
                                    case STARTED:
                                        snapshotStatus.abort();
                                        break;
                                    case FINALIZE:
                                        logger.debug("[{}] trying to cancel index on shard [{}] that is finalizing, letting it finish", entry.feed(), shard.key);
                                        break;
                                    case DONE:
                                        logger.debug("[{}] trying to cancel index on the shard [{}] that is already done, updating status on the master", entry.feed(), shard.key);
                                        updateIndexShardPushingStatus(entry.feed(), shard.key,
                                                new FeedInProgress.ShardPushingStatus(localNodeId, FeedInProgress.State.SUCCESS), masterNode);
                                        break;
                                    case FAILURE:
                                        logger.error("[{}] trying to cancel index on the shard [{}] that has already failed, updating status on the master", entry.feed(), shard.key);
                                        updateIndexShardPushingStatus(entry.feed(), shard.key,
                                                new FeedInProgress.ShardPushingStatus(localNodeId, FeedInProgress.State.FAILED, snapshotStatus.failure()), masterNode);
                                        break;
                                    default:
                                        throw new IllegalStateException("Unknown index shard stage " + snapshotStatus.stage());
                                }
                            }
                        }
                    }
                }
            }
        }

        // We have new shards to starts
        if (newSnapshots.isEmpty() == false) {
            for (final Map.Entry<Feed, Map<ShardId, IndexShardPushingStatus>> entry : newSnapshots.entrySet()) {
                Map<String, IndexId> indicesMap = snapshotIndices.get(entry.getKey());
                assert indicesMap != null;
                for (final Map.Entry<ShardId, IndexShardPushingStatus> shardEntry : entry.getValue().entrySet()) {
                    final ShardId shardId = shardEntry.getKey();
                    try {
                        final IndexShard indexShard = indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
                        final IndexId indexId = indicesMap.get(shardId.getIndexName());
                        assert indexId != null;
                        updateIndexShardPushingStatus(entry.getKey(), shardId, new FeedInProgress.ShardPushingStatus(localNodeId, FeedInProgress.State.STARTED), masterNode);
                        schedulePushingOnShard(indexShard, entry.getKey(),
                                // onFailure
                                (e) -> {
                                    if (logger.isDebugEnabled()) {
                                        logger.debug(() -> new ParameterizedMessage("failed to scheduled xdcr shard [{}] on thread pool [{}]", shardId), e);
                                    }
                                    updateIndexShardPushingStatus(entry.getKey(), shardId,
                                            new FeedInProgress.ShardPushingStatus(localNodeId, FeedInProgress.State.FAILED, ExceptionsHelper.detailedMessage(e)), masterNode);
                                });
                    } catch (Exception e) {
                        updateIndexShardPushingStatus(entry.getKey(), shardId,
                                new FeedInProgress.ShardPushingStatus(localNodeId, FeedInProgress.State.FAILED, ExceptionsHelper.detailedMessage(e)), masterNode);
                    }
                }
            }
        }

        // Update the list of snapshots that we saw and tried to started
        // If startup of these shards fails later, we don't want to try starting these shards again
        shutdownLock.lock();
        try {
            shardPushings = unmodifiableMap(survivors);
            if (shardPushings.isEmpty()) {
                // Notify all waiting threads that no more snapshots
                shutdownCondition.signalAll();
            }
        } finally {
            shutdownLock.unlock();
        }
    }

    /**
     * Creates shard index startRecovery
     */
    private void schedulePushingOnShard(final IndexShard indexShard, final Feed feed, final Consumer<Exception> failureConsumer) {
        ShardId shardId = indexShard.shardId();
        if (!indexShard.routingEntry().primary()) {
            throw new IndexShardPushingException(shardId, "pushing should be performed only on primary");
        }
        if (indexShard.routingEntry().relocating()) {
            // do not index when in the process of relocation of primaries so we won't get conflicts
            throw new IndexShardPushingException(shardId, "cannot puhsing while relocating");
        }
        if (indexShard.state() == IndexShardState.CREATED || indexShard.state() == IndexShardState.RECOVERING) {
            // shard has just been created, or still startRecovery
            throw new IndexShardPushingException(shardId, "shard didn't fully recover yet");
        }

        taskService.schedule(feed.getRepository(), shardId.getIndexName(), indexShard, failureConsumer);
    }

    /**
     * Checks if any shards were processed that the new master doesn't know about
     */
    private void syncShardStatsOnNewMaster(ClusterChangedEvent event) {
        FeedInProgress feedInProgress = event.state().custom(FeedInProgress.TYPE);
        if (feedInProgress == null) {
            return;
        }
        final String localNodeId = event.state().nodes().getLocalNodeId();
        final DiscoveryNode masterNode = event.state().nodes().getMasterNode();
        for (FeedInProgress.Entry snapshot : feedInProgress.entries()) {
            if (snapshot.state() == FeedInProgress.State.STARTED || snapshot.state() == FeedInProgress.State.ABORTED) {
                Map<ShardId, IndexShardPushingStatus> localShards = currentPushingShards(snapshot.feed());
                if (localShards != null) {
                    ImmutableOpenMap<ShardId, FeedInProgress.ShardPushingStatus> masterShards = snapshot.shards();
                    for (Map.Entry<ShardId, IndexShardPushingStatus> localShard : localShards.entrySet()) {
                        ShardId shardId = localShard.getKey();
                        IndexShardPushingStatus localShardStatus = localShard.getValue();
                        FeedInProgress.ShardPushingStatus masterShard = masterShards.get(shardId);
                        if (masterShard != null && masterShard.state().completed() == false) {
                            // Master knows about the shard and thinks it has not completed
                            if (localShardStatus.stage() == IndexShardPushingStatus.Stage.DONE) {
                                // but we think the shard is done - we need to make new master know that the shard is done
                                logger.debug("[{}] new master thinks the shard [{}] is not completed but the shard is done locally, updating status on the master", snapshot.feed(), shardId);
                                updateIndexShardPushingStatus(snapshot.feed(), shardId,
                                        new FeedInProgress.ShardPushingStatus(localNodeId, FeedInProgress.State.SUCCESS), masterNode);
                            } else if (localShard.getValue().stage() == IndexShardPushingStatus.Stage.FAILURE) {
                                // but we think the shard failed - we need to make new master know that the shard failed
                                logger.debug("[{}] new master thinks the shard [{}] is not completed but the shard failed locally, updating status on master", snapshot.feed(), shardId);
                                updateIndexShardPushingStatus(snapshot.feed(), shardId,
                                        new FeedInProgress.ShardPushingStatus(localNodeId, FeedInProgress.State.FAILED, localShardStatus.failure()), masterNode);

                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Stores the list of shards that has to be snapshotted on this node
     */
    private static class PushingShards {
        private Map<ShardId, IndexShardPushingStatus> shards;

        private PushingShards(Map<ShardId, IndexShardPushingStatus> shards) {
            this.shards = shards;
        }
    }


    /**
     * Internal request that is used to send changes in index status to master
     */
    public static class UpdateIndexShardSnapshotStatusRequest extends MasterNodeRequest<UpdateIndexShardSnapshotStatusRequest> {
        private Feed feed;
        private ShardId shardId;
        private FeedInProgress.ShardPushingStatus status;

        public UpdateIndexShardSnapshotStatusRequest() {

        }

        public UpdateIndexShardSnapshotStatusRequest(Feed feed, ShardId shardId, FeedInProgress.ShardPushingStatus status) {
            this.feed = feed;
            this.shardId = shardId;
            this.status = status;
            // By default, we keep trying to post index status messages to avoid index processes getting stuck.
            this.masterNodeTimeout = TimeValue.timeValueNanos(Long.MAX_VALUE);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            feed = new Feed(in);
            shardId = ShardId.readShardId(in);
            status = new FeedInProgress.ShardPushingStatus(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            feed.writeTo(out);
            shardId.writeTo(out);
            status.writeTo(out);
        }

        public Feed snapshot() {
            return feed;
        }

        public ShardId shardId() {
            return shardId;
        }

        public FeedInProgress.ShardPushingStatus status() {
            return status;
        }

        @Override
        public String toString() {
            return feed + ", shardId [" + shardId + "], status [" + status.state() + "]";
        }
    }

    /**
     * Updates the shard status
     */
    public void updateIndexShardPushingStatus(Feed snapshot, ShardId shardId, FeedInProgress.ShardPushingStatus status, DiscoveryNode master) {
        try {
            if (master.getVersion().onOrAfter(Version.V_6_1_0)) {
                UpdateIndexShardSnapshotStatusRequest request = new UpdateIndexShardSnapshotStatusRequest(snapshot, shardId, status);
                transportService.sendRequest(transportService.getLocalNode(), UPDATE_SNAPSHOT_STATUS_ACTION_NAME, request, EmptyTransportResponseHandler.INSTANCE_SAME);
            } else {
                UpdatePushingStatusRequestV6 requestV6 = new UpdatePushingStatusRequestV6(snapshot, shardId, status);
                transportService.sendRequest(master, UPDATE_SNAPSHOT_STATUS_ACTION_NAME_V6, requestV6, EmptyTransportResponseHandler.INSTANCE_SAME);
            }
        } catch (Exception e) {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}] [{}] failed to createOrUpdate index state", snapshot, status), e);
        }
    }

    /**
     * Updates the shard status on master node
     *
     * @param request createOrUpdate shard status request
     */
    private void innerUpdatePushingState(final UpdateIndexShardSnapshotStatusRequest request, ActionListener<UpdateIndexShardPushingStatusResponse> listener) {
        logger.trace("received updated index restore state [{}]", request);
        clusterService.submitStateUpdateTask(
                "createOrUpdate index state",
                request,
                ClusterStateTaskConfig.build(Priority.NORMAL),
                pushingStateExecutor,
                new ClusterStateTaskListener() {
                    @Override
                    public void onFailure(String source, Exception e) {
                        listener.onFailure(e);
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        listener.onResponse(new UpdateIndexShardPushingStatusResponse());
                    }
                });
    }

    class PushingStateExecutor implements ClusterStateTaskExecutor<UpdateIndexShardSnapshotStatusRequest> {

        @Override
        public ClusterTasksResult<UpdateIndexShardSnapshotStatusRequest> execute(ClusterState currentState, List<UpdateIndexShardSnapshotStatusRequest> tasks) throws Exception {
            final FeedInProgress snapshots = currentState.custom(FeedInProgress.TYPE);
            if (snapshots != null) {
                int changedCount = 0;
                final List<FeedInProgress.Entry> entries = new ArrayList<>();
                for (FeedInProgress.Entry entry : snapshots.entries()) {
                    ImmutableOpenMap.Builder<ShardId, FeedInProgress.ShardPushingStatus> shards = ImmutableOpenMap.builder();
                    boolean updated = false;

                    for (UpdateIndexShardSnapshotStatusRequest updateSnapshotState : tasks) {
                        if (entry.feed().equals(updateSnapshotState.snapshot())) {
                            logger.trace("[{}] Updating shard [{}] with status [{}]", updateSnapshotState.snapshot(), updateSnapshotState.shardId(), updateSnapshotState.status().state());
                            if (updated == false) {
                                shards.putAll(entry.shards());
                                updated = true;
                            }
                            shards.put(updateSnapshotState.shardId(), updateSnapshotState.status());
                            changedCount++;
                        }
                    }

                    if (updated) {
                        if (FeedInProgress.completed(shards.values()) == false) {
                            entries.add(new FeedInProgress.Entry(entry, shards.build()));
                        } else {
                            FeedInProgress.Entry updatedEntry = new FeedInProgress.Entry(entry, FeedInProgress.State.SUCCESS, shards.build());
                            entries.add(updatedEntry);
                        }
                    } else {
                        entries.add(entry);
                    }
                }
                if (changedCount > 0) {
                    logger.trace("changed cluster state triggered by {} index state updates", changedCount);

                    final FeedInProgress updatedSnapshots = new FeedInProgress(entries.toArray(new FeedInProgress.Entry[entries.size()]));
                    return ClusterTasksResult.<UpdateIndexShardSnapshotStatusRequest>builder().successes(tasks).build(
                            ClusterState.builder(currentState).putCustom(FeedInProgress.TYPE, updatedSnapshots).build());
                }
            }
            return ClusterTasksResult.<UpdateIndexShardSnapshotStatusRequest>builder().successes(tasks).build(currentState);
        }
    }

    static class UpdateIndexShardPushingStatusResponse extends ActionResponse {

    }

    class UpdatePushingStatusAction extends TransportMasterNodeAction<UpdateIndexShardSnapshotStatusRequest, UpdateIndexShardPushingStatusResponse> {
        UpdatePushingStatusAction(Settings settings, String actionName, TransportService transportService, ClusterService clusterService,
                                  ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
            super(settings, actionName, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, UpdateIndexShardSnapshotStatusRequest::new);
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected UpdateIndexShardPushingStatusResponse newResponse() {
            return new UpdateIndexShardPushingStatusResponse();
        }

        @Override
        protected void masterOperation(UpdateIndexShardSnapshotStatusRequest request, ClusterState state, ActionListener<UpdateIndexShardPushingStatusResponse> listener) throws Exception {
            innerUpdatePushingState(request, listener);
        }

        @Override
        protected ClusterBlockException checkBlock(UpdateIndexShardSnapshotStatusRequest request, ClusterState state) {
            return null;
        }
    }

    /**
     * A BWC version of {@link UpdateIndexShardSnapshotStatusRequest}
     */
    static class UpdatePushingStatusRequestV6 extends TransportRequest {
        private Feed snapshot;
        private ShardId shardId;
        private FeedInProgress.ShardPushingStatus status;

        UpdatePushingStatusRequestV6() {

        }

        UpdatePushingStatusRequestV6(Feed snapshot, ShardId shardId, FeedInProgress.ShardPushingStatus status) {
            this.snapshot = snapshot;
            this.shardId = shardId;
            this.status = status;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            snapshot = new Feed(in);
            shardId = ShardId.readShardId(in);
            status = new FeedInProgress.ShardPushingStatus(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            snapshot.writeTo(out);
            shardId.writeTo(out);
            status.writeTo(out);
        }

        Feed snapshot() {
            return snapshot;
        }

        ShardId shardId() {
            return shardId;
        }

        FeedInProgress.ShardPushingStatus status() {
            return status;
        }

        @Override
        public String toString() {
            return snapshot + ", shardId [" + shardId + "], status [" + status.state() + "]";
        }
    }

    /**
     * A BWC version of {@link UpdatePushingStatusAction}
     */
    class UpdatePushingStateRequestHandlerV6 implements TransportRequestHandler<UpdatePushingStatusRequestV6> {
        @Override
        public void messageReceived(UpdatePushingStatusRequestV6 requestV6, final TransportChannel channel) throws Exception {
            final UpdateIndexShardSnapshotStatusRequest request = new UpdateIndexShardSnapshotStatusRequest(requestV6.snapshot(), requestV6.shardId(), requestV6.status());
            innerUpdatePushingState(request, new ActionListener<UpdateIndexShardPushingStatusResponse>() {
                @Override
                public void onResponse(UpdateIndexShardPushingStatusResponse updateSnapshotStatusResponse) {

                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn("Failed to createOrUpdate index status", e);
                }
            });
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

}
