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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.vpack.xdcr.metadata.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class PushingService extends AbstractLifecycleComponent implements ClusterStateApplier {

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final ThreadPool threadPool;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final CopyOnWriteArrayList<SnapshotCompletionListener> snapshotCompletionListeners = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<String, Scheduler.Cancellable> scheduledTasks = new ConcurrentHashMap<>();

    @Inject
    public PushingService(Settings settings, ClusterService clusterService, TransportService transportService,
                          IndexNameExpressionResolver indexNameExpressionResolver, ThreadPool threadPool) {
        super(settings);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.threadPool = threadPool;
        if (DiscoveryNode.isMasterNode(settings)) {
            clusterService.addLowPriorityApplier(this);
        }
    }

    /** =============================
     *  创建 Feed startRecovery 流 Start
     *  =============================
     */

    /**
     * Initializes the startPuhsing process.
     * <p>
     * This method is used by clients to start index. It makes sure that there is no pushings are currently running and
     * creates a index record in cluster state metadata.
     *
     * @param request  index request
     * @param listener index creation listener
     */
    public void createPushingState(final FeedRequest request, final CreateFeedListener listener) {

        if (!clusterService.state().metaData().hasIndex(request.indices)) {
            throw new IndexNotFoundException("cross replication failed", request.indices);
        }

        Transport.Connection connection = transportService.getRemoteClusterService().getConnection(request.repository);
        if (connection == null) {
            throw new IllegalArgumentException("no such remote cluster: " + request.repository);
        }

        final FeedId feedId = new FeedId(request.indices, UUIDs.randomBase64UUID());

        // ClusterState 添加每个shard任务 驱动 PushingShardService 执行shard级别同步
        clusterService.submitStateUpdateTask(request.cause(), new ClusterStateUpdateTask() {

            private FeedInProgress.Entry newTask = null;

            @Override
            public ClusterState execute(ClusterState currentState) {
                newTask = newFeed(currentState);
                FeedInProgress totalProgress = currentState.custom(FeedInProgress.TYPE);
                if (totalProgress == null || totalProgress.entries().isEmpty()) {
                    totalProgress = new FeedInProgress(newTask);
                } else {
                    FeedInProgress.Entry feedEntry = find(totalProgress, request.repository);
                    if (feedEntry != null) {
                        List<String> requestIndices = Arrays.asList(indexNameExpressionResolver.concreteIndexNames(currentState, request.indicesOptions(), request.indices()));

                        // validate xdcr settings enabled
                        validateXdcrEnabled(requestIndices, currentState);

                        // validate pushing index existed
                        List<String> indexExisted = requestIndices.stream().filter(
                                index -> feedEntry.indices().stream().anyMatch(indexId -> indexId.getName().equals(index)))
                                .collect(Collectors.toList());
                        if (!indexExisted.isEmpty()) {
                            throw new ConcurrentPushingExecutionException(request.repository, indexExisted.toString(), " a index is already running");
                        }

                        // pushing entry creation
                        int len = totalProgress.entries().size();
                        FeedInProgress.Entry[] newEntries = new FeedInProgress.Entry[len + 1];
                        for (int i = 0; i < len; i++) {
                            newEntries[i] = totalProgress.entries().get(i);
                        }
                        newEntries[len] = newTask;
                        totalProgress = new FeedInProgress(newEntries);
                    } else {
                        totalProgress.entries().add(newTask);
                    }
                }
                return ClusterState.builder(currentState).putCustom(FeedInProgress.TYPE, totalProgress).build();
            }

            private FeedInProgress.Entry find(FeedInProgress feedInProgress, String repository) {
                for (FeedInProgress.Entry entry : feedInProgress.entries()) {
                    if (entry.feed() == null || entry.feed().getRepository() == null) {
                        continue;
                    }
                    if (repository.equals(entry.feed().getRepository())) {
                        return entry;
                    }
                }
                return null;
            }

            private FeedInProgress.Entry newFeed(ClusterState currentState) {
                List<String> indices = Arrays.asList(indexNameExpressionResolver.concreteIndexNames(currentState, request.indicesOptions(), request.indices()));
                validateXdcrEnabled(indices, currentState);
                logger.trace("[{}][{}] creating feed startRecovery for index [{}]", request.repository, request.indices, indices);
                List<IndexId> pushingIndices = resolveNewIndices(indices);
                return new FeedInProgress.Entry(new Feed(request.repository, feedId),
                        FeedInProgress.State.INIT,
                        pushingIndices,
                        System.currentTimeMillis(),
                        0,
                        null);
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}][{}] failed to create index", request.repository, request.indices), e);
                newTask = null;
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, final ClusterState newState) {
                if (newTask != null) {

                    // 注册cluster_state 开始每个shard推送
                    threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> beginPushing(newState, newTask, listener));
                }
            }

            @Override
            public TimeValue timeout() {
                return request.masterNodeTimeout();
            }

        });
    }

    /**
     * Starts index.
     * <p>
     * Creates index in repository and updates index metadata record with list of shards that needs to be processed.
     *
     * @param clusterState           cluster state
     * @param feedEntry              index meta data
     * @param userCreateFeedListener listener
     */
    private void beginPushing(final ClusterState clusterState,
                              final FeedInProgress.Entry feedEntry,
                              final CreateFeedListener userCreateFeedListener) {
        boolean pushingCreated = false;
        try {
            logger.info("index [{}] started", feedEntry.feed());
            if (feedEntry.indices().isEmpty()) {
                // No indices in this index - we are done
                userCreateFeedListener.onResponse();
                endPushing(feedEntry);
                return;
            }
            clusterService.submitStateUpdateTask("update_pushing [" + feedEntry.feed() + "]", new ClusterStateUpdateTask() {
                boolean accepted = false;
                FeedInProgress.Entry updatedPushing;
                String failure = null;

                @Override
                public ClusterState execute(ClusterState currentState) {
                    FeedInProgress feedInProgress = currentState.custom(FeedInProgress.TYPE);
                    List<FeedInProgress.Entry> entries = new ArrayList<>();
                    for (FeedInProgress.Entry entry : feedInProgress.entries()) {
                        if (entry.feed().equals(feedEntry.feed())) {
                            ImmutableOpenMap<ShardId, FeedInProgress.ShardPushingStatus> shards = shards(currentState, entry.indices());
                            updatedPushing = new FeedInProgress.Entry(entry, FeedInProgress.State.STARTED, shards);
                            entries.add(updatedPushing);
                            if (!FeedInProgress.completed(shards.values())) {
                                accepted = true;
                            }
                        } else {
                            entries.add(entry);
                        }
                    }
                    return ClusterState.builder(currentState)
                            .putCustom(FeedInProgress.TYPE, new FeedInProgress(Collections.unmodifiableList(entries)))
                            .build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}] failed to createOrUpdate index", feedEntry.feed().getFeedId()), e);
                    removePushingFromClusterState(feedEntry.feed(), null, e, new CleanupAfterErrorListener(feedEntry, true, userCreateFeedListener, e));
                }

                @Override
                public void onNoLongerMaster(String source) {
                    // We are not longer a master - we shouldn't try to do any cleanup
                    // The new master will take care of it
                    logger.warn("[{}] failed to createOrUpdate index - no longer a master", feedEntry.feed().getFeedId());
                    userCreateFeedListener.onFailure(new PushingException(feedEntry.feed(), "master changed during index initialization"));
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    userCreateFeedListener.onResponse();
                    if (!accepted && updatedPushing != null) {
                        endPushing(updatedPushing, failure);
                    }
                }
            });
        } catch (Exception e) {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to createOrUpdate index [{}]", feedEntry.feed().getFeedId()), e);
            removePushingFromClusterState(feedEntry.feed(), null, e, new CleanupAfterErrorListener(feedEntry, pushingCreated, userCreateFeedListener, e));
        }
    }


    private class CleanupAfterErrorListener implements ActionListener<FeedInfo> {

        private final FeedInProgress.Entry snapshot;
        private final boolean snapshotCreated;
        private final CreateFeedListener userCreateFeedListener;
        private final Exception e;

        CleanupAfterErrorListener(FeedInProgress.Entry snapshot, boolean snapshotCreated, CreateFeedListener userCreateFeedListener, Exception e) {
            this.snapshot = snapshot;
            this.snapshotCreated = snapshotCreated;
            this.userCreateFeedListener = userCreateFeedListener;
            this.e = e;
        }

        @Override
        public void onResponse(FeedInfo feedInfo) {
            cleanupAfterError(this.e);
        }

        @Override
        public void onFailure(Exception e) {
            e.addSuppressed(this.e);
            cleanupAfterError(e);
        }

        public void onNoLongerMaster(String source) {
            userCreateFeedListener.onFailure(e);
        }

        private void cleanupAfterError(Exception exception) {
            userCreateFeedListener.onFailure(e);
        }

    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        try {
            if (event.localNodeMaster()) {
                if (event.nodesRemoved() || event.previousState().nodes().isLocalNodeElectedMaster() == false) {
                    processPushingsOnRemovedNodes(event);
                }

                // 出现FAILED
                FeedInProgress curr = event.state().custom(FeedInProgress.TYPE);
                if (curr == null) {
                    return;
                } else if (curr.waitingShardExisted()){
                    processChangedShards(event);
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to createOrUpdate index state ", e);
        }
    }


    /**
     * Cleans up shard pushings that were running on removed nodes
     *
     * @param event cluster changed event
     */
    private void processPushingsOnRemovedNodes(ClusterChangedEvent event) {
        if (removedNodesCleanupNeeded(event)) {
            // Check if we just became the master
            final boolean newMaster = !event.previousState().nodes().isLocalNodeElectedMaster();
            clusterService.submitStateUpdateTask("createOrUpdate index state after node removal", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    DiscoveryNodes nodes = currentState.nodes();
                    FeedInProgress feedInProgress = currentState.custom(FeedInProgress.TYPE);
                    if (feedInProgress == null) {
                        return currentState;
                    }
                    boolean changed = false;
                    ArrayList<FeedInProgress.Entry> entries = new ArrayList<>();
                    for (final FeedInProgress.Entry entry : feedInProgress.entries()) {
                        FeedInProgress.Entry updatedSnapshot = entry;
                        boolean pushingChanged = false;
                        if (entry.state() == FeedInProgress.State.STARTED || entry.state() == FeedInProgress.State.ABORTED) {
                            ImmutableOpenMap.Builder<ShardId, FeedInProgress.ShardPushingStatus> shards = ImmutableOpenMap.builder();
                            for (ObjectObjectCursor<ShardId, FeedInProgress.ShardPushingStatus> shardEntry : entry.shards()) {
                                FeedInProgress.ShardPushingStatus shardStatus = shardEntry.value;
                                if (!shardStatus.state().completed() && shardStatus.nodeId() != null) {
                                    if (nodes.nodeExists(shardStatus.nodeId())) {
                                        shards.put(shardEntry.key, shardEntry.value);
                                    } else {
                                        // 节点丢失
                                        pushingChanged = true;
                                        logger.warn("failing index of shard [{}] on closed node [{}]", shardEntry.key, shardStatus.nodeId());
                                        //shards.put(shardEntry.key, new ShardPushingStatus(shardStatus.nodeId(), State.FAILED, "node shutdown"));
                                        shards.put(shardEntry.key, shardPushingStatus(event.state(), shardEntry.key));
                                    }
                                }
                            }
                            if (pushingChanged) {
                                changed = true;
                                ImmutableOpenMap<ShardId, FeedInProgress.ShardPushingStatus> shardsMap = shards.build();
                                if (!entry.state().completed() && FeedInProgress.completed(shardsMap.values())) {
                                    updatedSnapshot = new FeedInProgress.Entry(entry, FeedInProgress.State.SUCCESS, shardsMap);
                                    endPushing(updatedSnapshot);
                                } else {
                                    updatedSnapshot = new FeedInProgress.Entry(entry, entry.state(), shardsMap);
                                }
                            }
                            entries.add(updatedSnapshot);
                        } else if (entry.state() == FeedInProgress.State.INIT && newMaster) {
                            // Clean up the index that failed to start from the old master
                            deleteFeed(entry.feed().key(), new DeleteFeedListener() {
                                @Override
                                public void onResponse() {
                                    logger.debug("cleaned up abandoned index {} in INIT state", entry.feed());
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    logger.warn("failed to clean up abandoned index {} in INIT state", entry.feed());
                                }
                            });
                        }
                    }
                    if (changed) {
                        feedInProgress = new FeedInProgress(entries.toArray(new FeedInProgress.Entry[entries.size()]));
                        return ClusterState.builder(currentState).putCustom(FeedInProgress.TYPE, feedInProgress).build();
                    }
                    return currentState;
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn("failed to createOrUpdate index state after node removal");
                }
            });
        }
    }

    private void processChangedShards(ClusterChangedEvent event) {

        // 处理shard routing变更
        clusterService.submitStateUpdateTask("update pushing status", new ClusterStateUpdateTask() {

            @Override
            public ClusterState execute(ClusterState currentState) {
                RoutingTable routingTable = currentState.routingTable();
                FeedInProgress feedInProgress = currentState.custom(FeedInProgress.TYPE);
                ArrayList<FeedInProgress.Entry> entries = new ArrayList<>();
                boolean changed = false;
                for (final FeedInProgress.Entry entry : feedInProgress.entries()) {
                    FeedInProgress.Entry updatedEntry = entry;
                    if (entry.state() == FeedInProgress.State.STARTED) {
                        // 处理shard更新后的pushing状态变更.
                        ImmutableOpenMap<ShardId, FeedInProgress.ShardPushingStatus> shards = processUnassignedShards(entry.shards(), routingTable);
                        if (shards != null) {
                            changed = true;
                            updatedEntry = new FeedInProgress.Entry(entry, shards);
                        }
                        entries.add(updatedEntry);
                    } else {
                        entries.add(entry);
                    }
                }
                if (changed) {
                    feedInProgress = new FeedInProgress(entries.toArray(new FeedInProgress.Entry[entries.size()]));
                    return ClusterState.builder(currentState).putCustom(FeedInProgress.TYPE, feedInProgress).build();
                }
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to createOrUpdate index state after shards started from [{}] ", source), e);
            }
        });
    }

    private ImmutableOpenMap<ShardId, FeedInProgress.ShardPushingStatus> processUnassignedShards(ImmutableOpenMap<ShardId, FeedInProgress.ShardPushingStatus> pushingShards, RoutingTable routingTable) {
        boolean snapshotChanged = false;
        ImmutableOpenMap.Builder<ShardId, FeedInProgress.ShardPushingStatus> shards = ImmutableOpenMap.builder();
        for (ObjectObjectCursor<ShardId, FeedInProgress.ShardPushingStatus> shardEntry : pushingShards) {
            FeedInProgress.ShardPushingStatus shardStatus = shardEntry.value;
            ShardId shardId = shardEntry.key;
            if (shardStatus.state() == FeedInProgress.State.MISSING
                    || shardStatus.state() == FeedInProgress.State.FAILED
                    || shardStatus.state() == FeedInProgress.State.ABORTED
                    || shardStatus.state() == FeedInProgress.State.WAITING) {
                IndexRoutingTable indexShardRoutingTable = routingTable.index(shardId.getIndex());
                if (indexShardRoutingTable != null) {
                    IndexShardRoutingTable shardRouting = indexShardRoutingTable.shard(shardId.id());
                    if (shardRouting != null && shardRouting.primaryShard() != null) {
                        if (shardRouting.primaryShard().started()) {
                            snapshotChanged = true;
                            shards.put(shardId, new FeedInProgress.ShardPushingStatus(shardRouting.primaryShard().currentNodeId()));
                            continue;
                        } else if (shardRouting.primaryShard().initializing() || shardRouting.primaryShard().relocating()) {
                            shards.put(shardId, shardStatus);
                            continue;
                        }
                    }
                }
                // Shard that we were waiting for went into unassigned state or disappeared - giving up
                snapshotChanged = true;
                logger.warn("failing index of shard [{}] on unassigned shard [{}]", shardId, shardStatus.nodeId());
                shards.put(shardId, new FeedInProgress.ShardPushingStatus(shardStatus.nodeId(), FeedInProgress.State.FAILED, "shard is unassigned"));
            } else {
                shards.put(shardId, shardStatus);
            }
        }
        if (snapshotChanged) {
            return shards.build();
        } else {
            return null;
        }
    }

    private boolean removedNodesCleanupNeeded(ClusterChangedEvent event) {
        FeedInProgress feedInProgress = event.state().custom(FeedInProgress.TYPE);
        if (feedInProgress == null) {
            return false;
        }
        // Check if we just became the master
        boolean newMaster = !event.previousState().nodes().isLocalNodeElectedMaster();
        for (FeedInProgress.Entry snapshot : feedInProgress.entries()) {
            if (newMaster && (snapshot.state() == FeedInProgress.State.SUCCESS || snapshot.state() == FeedInProgress.State.INIT)) {
                // We just replaced old master and snapshots in intermediate states needs to be cleaned
                return true;
            }
            for (DiscoveryNode node : event.nodesDelta().removedNodes()) {
                for (ObjectCursor<FeedInProgress.ShardPushingStatus> shardStatus : snapshot.shards().values()) {
                    if (!shardStatus.value.state().completed() && node.getId().equals(shardStatus.value.nodeId())) {
                        // At least one shard was running on the removed node - we need to fail it
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Returns list of indices with missing shards, and list of indices that are closed
     *
     * @param shards list of shard statuses
     * @return list of failed and closed indices
     */
    private Tuple<Set<String>, Set<String>> indicesWithMissingShards(ImmutableOpenMap<ShardId, FeedInProgress.ShardPushingStatus> shards, MetaData metaData) {
        Set<String> missing = new HashSet<>();
        Set<String> closed = new HashSet<>();
        for (ObjectObjectCursor<ShardId, FeedInProgress.ShardPushingStatus> entry : shards) {
            if (entry.value.state() == FeedInProgress.State.MISSING) {
                if (metaData.hasIndex(entry.key.getIndex().getName()) && metaData.getIndexSafe(entry.key.getIndex()).getState() == IndexMetaData.State.CLOSE) {
                    closed.add(entry.key.getIndex().getName());
                } else {
                    missing.add(entry.key.getIndex().getName());
                }
            }
        }
        return new Tuple<>(missing, closed);
    }

    /**
     * Finalizes the shard in repository and then removes it from cluster state
     * <p>
     * This is non-blocking method that runs on a thread from SNAPSHOT thread pool
     *
     * @param entry index
     */
    void endPushing(FeedInProgress.Entry entry) {
        endPushing(entry, null);
    }


    /**
     * Finalizes the shard in repository and then removes it from cluster state
     * <p>
     * This is non-blocking method that runs on a thread from SNAPSHOT thread pool
     *
     * @param entry   index
     * @param failure failure reason or null if index was successful
     */
    private void endPushing(final FeedInProgress.Entry entry, final String failure) {
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
            final Feed feed = entry.feed();
            try {
                logger.trace("[{}] finalizing index in repository, state: [{}], failure[{}]", feed, entry.state(), failure);
                ArrayList<PushingShardFailure> shardFailures = new ArrayList<>();
                for (ObjectObjectCursor<ShardId, FeedInProgress.ShardPushingStatus> shardStatus : entry.shards()) {
                    ShardId shardId = shardStatus.key;
                    FeedInProgress.ShardPushingStatus status = shardStatus.value;
                    if (status.state().failed()) {
                        shardFailures.add(new PushingShardFailure(status.nodeId(), shardId, status.reason()));
                    }
                }
                FeedInfo feedInfo = finalizePushing(
                        feed.getFeedId(),
                        entry.indices(),
                        entry.startTime(),
                        failure,
                        entry.shards().size(),
                        Collections.unmodifiableList(shardFailures),
                        entry.getRepositoryStateId());
                removePushingFromClusterState(feed, feedInfo, null, null);
                logger.info("index [{}] completed with state [{}]", feed, feedInfo.state());
            } catch (Exception e) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("[{}] failed to finalize index", feed), e);
                removePushingFromClusterState(feed, null, e, null);
            }
        });
    }

    /**
     * Removes record of running index from cluster state and notifies the listener when this action is complete
     *
     * @param feed     index
     * @param failure  exception if index failed
     * @param listener listener to notify when index information is removed from the cluster state
     */
    private void removePushingFromClusterState(final Feed feed, final FeedInfo feedInfo, final Exception failure,
                                               @Nullable CleanupAfterErrorListener listener) {
        clusterService.submitStateUpdateTask("remove pushing task", new ClusterStateUpdateTask() {

            @Override
            public ClusterState execute(ClusterState currentState) {
                FeedInProgress feedInProgress = currentState.custom(FeedInProgress.TYPE);
                if (feedInProgress != null) {
                    boolean changed = false;
                    ArrayList<FeedInProgress.Entry> entries = new ArrayList<>();
                    for (FeedInProgress.Entry entry : feedInProgress.entries()) {
                        if (entry.feed().equals(feed)) {
                            changed = true;
                        } else {
                            entries.add(entry);
                        }
                    }
                    if (changed) {
                        feedInProgress = new FeedInProgress(entries.toArray(new FeedInProgress.Entry[entries.size()]));
                        return ClusterState.builder(currentState).putCustom(FeedInProgress.TYPE, feedInProgress).build();
                    }
                }
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}] failed to remove index metadata", feed), e);
                if (listener != null) {
                    listener.onFailure(e);
                }
            }

            @Override
            public void onNoLongerMaster(String source) {
                if (listener != null) {
                    listener.onNoLongerMaster(source);
                }
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                for (SnapshotCompletionListener listener : snapshotCompletionListeners) {
                    try {
                        if (feedInfo != null) {
                            listener.onFeedPushingCompletion(feed, feedInfo);
                        } else {
                            listener.onFeedPushingFailure(feed, failure);
                        }
                    } catch (Exception t) {
                        logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to notify listener [{}]", listener), t);
                    }
                }
                if (listener != null) {
                    listener.onResponse(feedInfo);
                }
            }
        });
    }

    /**
     * Deletes feed startRecovery
     */
    public void deleteFeed(final String feedKey, final DeleteFeedListener listener) {
        FeedInProgress feedInProgress = clusterService.state().custom(FeedInProgress.TYPE);
        if (feedInProgress == null) {
            return;
        }
        List<FeedInProgress.Entry> entries = feedInProgress.entries().stream().filter(entry -> entry.feed().key().equals(feedKey)).collect(Collectors.toList());
        if (!entries.isEmpty()) {
            for (FeedInProgress.Entry entry : entries) {
                endPushing(entry);
            }
        }
        listener.onResponse();
        Scheduler.Cancellable task = scheduledTasks.get(feedKey);
        if (task != null) {
            task.cancel();
        }
        scheduledTasks.remove(feedKey);
    }

    /**
     * Checks if a repository is currently in use by one of the snapshots
     *
     * @param clusterState cluster state
     * @param repository   repository id
     * @return true if repository is currently in use by one of the running snapshots
     */
    public static boolean isRepositoryInUse(ClusterState clusterState, String repository) {
        FeedInProgress snapshots = clusterState.custom(FeedInProgress.TYPE);
        if (snapshots != null) {
            for (FeedInProgress.Entry snapshot : snapshots.entries()) {
                if (repository.equals(snapshot.feed().getRepository())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Calculates the list of shards that should be included into the current index
     *
     * @param clusterState cluster state
     * @param indices      list of indices to be snapshotted
     * @return list of shard to be included into current index
     */
    private ImmutableOpenMap<ShardId, FeedInProgress.ShardPushingStatus> shards(ClusterState clusterState, List<IndexId> indices) {
        ImmutableOpenMap.Builder<ShardId, FeedInProgress.ShardPushingStatus> builder = ImmutableOpenMap.builder();
        MetaData metaData = clusterState.metaData();
        for (IndexId index : indices) {
            final String indexName = index.getName();
            IndexMetaData indexMetaData = metaData.index(indexName);
            if (indexMetaData == null) {
                // The index was deleted before we managed to start the index - mark it as missing.
                builder.put(new ShardId(indexName, IndexMetaData.INDEX_UUID_NA_VALUE, 0), new FeedInProgress.ShardPushingStatus(null, FeedInProgress.State.MISSING, "missing index"));
            } else if (indexMetaData.getState() == IndexMetaData.State.CLOSE) {
                for (int i = 0; i < indexMetaData.getNumberOfShards(); i++) {
                    ShardId shardId = new ShardId(indexMetaData.getIndex(), i);
                    builder.put(shardId, new FeedInProgress.ShardPushingStatus(null, FeedInProgress.State.MISSING, "index is closed"));
                }
            } else {
                IndexRoutingTable indexRoutingTable = clusterState.getRoutingTable().index(indexName);
                for (int i = 0; i < indexMetaData.getNumberOfShards(); i++) {
                    ShardId shardId = new ShardId(indexMetaData.getIndex(), i);
                    if (indexRoutingTable != null) {
                        ShardRouting primary = indexRoutingTable.shard(i).primaryShard();
                        if (primary == null || !primary.assignedToNode()) {
                            builder.put(shardId, new FeedInProgress.ShardPushingStatus(null, FeedInProgress.State.MISSING, "primary shard is not allocated"));
                        } else if (primary.relocating() || primary.initializing()) {
                            builder.put(shardId, new FeedInProgress.ShardPushingStatus(primary.currentNodeId(), FeedInProgress.State.WAITING));
                        } else if (!primary.started()) {
                            builder.put(shardId, new FeedInProgress.ShardPushingStatus(primary.currentNodeId(), FeedInProgress.State.MISSING, "primary shard hasn't been started yet"));
                        } else {
                            builder.put(shardId, new FeedInProgress.ShardPushingStatus(primary.currentNodeId()));
                        }
                    } else {
                        builder.put(shardId, new FeedInProgress.ShardPushingStatus(null, FeedInProgress.State.MISSING, "missing routing table"));
                    }
                }
            }
        }
        return builder.build();
    }

    private FeedInProgress.ShardPushingStatus shardPushingStatus(ClusterState clusterState, ShardId shardId) {
        if (shardId.getIndex() == null) {
            return new FeedInProgress.ShardPushingStatus(null, FeedInProgress.State.MISSING, "primary shard is not allocated");
        }
        IndexRoutingTable indexRoutingTable = clusterState.getRoutingTable().index(shardId.getIndexName());
        if (indexRoutingTable == null) {
            return new FeedInProgress.ShardPushingStatus(null, FeedInProgress.State.MISSING, "primary shard is not allocated");
        } else {
            ShardRouting primary = indexRoutingTable.shard(shardId.id()).primaryShard();
            if (primary == null || !primary.assignedToNode()) {
                return new FeedInProgress.ShardPushingStatus(null, FeedInProgress.State.MISSING, "primary shard is not allocated");
            } else if (primary.relocating() || primary.initializing()) {
                return new FeedInProgress.ShardPushingStatus(primary.currentNodeId(), FeedInProgress.State.WAITING);
            } else if (!primary.started()) {
                return new FeedInProgress.ShardPushingStatus(primary.currentNodeId(), FeedInProgress.State.MISSING, "primary shard hasn't been started yet");
            } else {
                return new FeedInProgress.ShardPushingStatus(primary.currentNodeId());
            }
        }
    }


    public void validateXdcrEnabled(List<String> indices, ClusterState state) {
        // validateFormat xdcr settings enabled
        indices.forEach(index -> {
            if (!xdcrSettingEnabled(state, index)) {
                throw new IndexXdcrCreationException("index setting [index.xdcr.enabled] could not be null or false");
            }
        });
    }

//    private static void validateFormat(final String repository, final String index) {
//        if (Strings.hasLength(index) == false) {
//            throw new InvalidPushingNameException(repository, index, "cannot be empty");
//        }
//        if (index.contains(" ")) {
//            throw new InvalidPushingNameException(repository, index, "must not contain whitespace");
//        }
//        if (index.contains(",")) {
//            throw new InvalidPushingNameException(repository, index, "must not contain ','");
//        }
//        if (index.contains("#")) {
//            throw new InvalidPushingNameException(repository, index, "must not contain '#'");
//        }
//        if (index.charAt(0) == '_') {
//            throw new InvalidPushingNameException(repository, index, "must not start with '_'");
//        }
//        if (index.toLowerCase(Locale.ROOT).equals(index) == false) {
//            throw new InvalidPushingNameException(repository, index, "must be lowercase");
//        }
//        if (Strings.validFileName(index) == false) {
//            throw new InvalidPushingNameException(repository,
//                    index,
//                    "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
//        }
//    }
//
//    private void validateRemoteConnected(final String repository) {
//        Transport.Connection connection = transportService.getRemoteClusterService().getConnection(repository);
//        if (connection == null) {
//            throw new IllegalArgumentException("no such remote cluster: " + repository);
//        }
//    }

    private boolean xdcrSettingEnabled(ClusterState state, String index) {
        Settings settings = state.metaData().index(index).getSettings();
        boolean xdcrEnabled = settings.getAsBoolean("index.xdcr.enabled", false);
        return xdcrEnabled;
    }

    /**
     * Adds index completion listener
     *
     * @param listener listener
     */
    public void addListener(SnapshotCompletionListener listener) {
        this.snapshotCompletionListeners.add(listener);
    }

    /**
     * Removes index completion listener
     *
     * @param listener listener
     */
    public void removeListener(SnapshotCompletionListener listener) {
        this.snapshotCompletionListeners.remove(listener);
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {
        clusterService.removeApplier(this);
    }

    /**
     * Listener for createOrUpdate index operation
     */
    public interface CreateFeedListener {

        /**
         * Called when index has successfully started
         */
        void onResponse();

        /**
         * Called if a index operation couldn't start
         */
        void onFailure(Exception e);
    }

    /**
     * Listener for delete index operation
     */
    public interface DeleteFeedListener {

        /**
         * Called if delete operation was successful
         */
        void onResponse();

        /**
         * Called if delete operation failed
         */
        void onFailure(Exception e);
    }

    public interface SnapshotCompletionListener {

        void onFeedPushingCompletion(Feed snapshot, FeedInfo feedInfo);

        void onFeedPushingFailure(Feed snapshot, Exception e);
    }

    /**
     * Feed creation request
     */
    public static class FeedRequest {

        private final String cause;

        private final String repository;

        private final String indices;

        private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();

        private Settings settings;

        private TimeValue masterNodeTimeout;

        /**
         * Constructs new index creation request
         *
         * @param repository repository getName
         * @param indices    index getName
         * @param cause      cause for index operation
         */
        public FeedRequest(final String repository, final String indices, final String cause) {
            this.repository = Objects.requireNonNull(repository);
            this.indices = Objects.requireNonNull(indices);
            this.cause = Objects.requireNonNull(cause);
        }


        /**
         * Sets repository-specific index settings
         *
         * @param settings index settings
         * @return this request
         */
        public FeedRequest settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        /**
         * Sets master node timeout
         *
         * @param masterNodeTimeout master node timeout
         * @return this request
         */
        public FeedRequest masterNodeTimeout(TimeValue masterNodeTimeout) {
            this.masterNodeTimeout = masterNodeTimeout;
            return this;
        }

        /**
         * Sets the indices options
         *
         * @param indicesOptions indices options
         * @return this request
         */
        public FeedRequest indicesOptions(IndicesOptions indicesOptions) {
            this.indicesOptions = indicesOptions;
            return this;
        }

        /**
         * Returns cause for index operation
         *
         * @return cause for index operation
         */
        public String cause() {
            return cause;
        }

        /**
         * Returns the repository getName
         */
        public String repositoryName() {
            return repository;
        }

        /**
         * Returns the index getName
         */
        public String indices() {
            return indices;
        }

        /**
         * Returns indices options
         *
         * @return indices options
         */
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        /**
         * Returns repository-specific settings for the index operation
         *
         * @return repository-specific settings
         */
        public Settings settings() {
            return settings;
        }

        /**
         * Returns master node timeout
         *
         * @return master node timeout
         */
        public TimeValue masterNodeTimeout() {
            return masterNodeTimeout;
        }

    }


    private List<IndexId> resolveNewIndices(final List<String> indicesToResolve) {
        List<IndexId> indices = new ArrayList<>();
        for (String index : indicesToResolve) {
            final IndexId indexId;
            indexId = new IndexId(index, UUIDs.randomBase64UUID());
            indices.add(indexId);
        }
        return indices;
    }

    private FeedInfo finalizePushing(FeedId feedId, List<IndexId> indices, long startTime, String failure, int totalShards,
                                     List<PushingShardFailure> shardFailures, long repositoryStateId) {
        FeedInfo feedInfo = new FeedInfo(feedId,
                indices.stream().map(IndexId::getName).collect(Collectors.toList()),
                startTime, failure, System.currentTimeMillis(), totalShards, shardFailures);
        return feedInfo;
    }
}

