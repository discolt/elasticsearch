package org.elasticsearch.vpack.xdcr.core;

import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.vpack.xdcr.core.follower.FollowerHandlerRegister;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportFuture;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;


public class RemoteNodeProxy {

    private final TransportService transportService;
    private final ShardId sourceShardId;
    private ThreadPool threadPool;
    private final String repository;

    public RemoteNodeProxy(TransportService transportService, ThreadPool threadPool, String repository, ShardId sourceShardId) {
        this.transportService = transportService;
        this.sourceShardId = sourceShardId;
        this.repository = repository;
        this.threadPool = threadPool;
    }

    private Client remoteClient() {
        return transportService.getRemoteClusterService().getRemoteClusterClient(threadPool, repository);
    }

    private DiscoveryNode remoteNode() {
        return transportService.getRemoteClusterService().getConnection(repository).getNode();
    }

    public String getRepository() {
        return repository;
    }

    public ShardId getShardId() {
        return sourceShardId;
    }

    /**
     * Fetch globalCheckpoint on follower cluster
     *
     * @return GlobalCheckpoint
     */
    public long fetchGlobalCheckpoint() {
        IndicesStatsResponse resp = remoteClient().admin().indices().stats(new IndicesStatsRequest().indices(sourceShardId.getIndexName())).actionGet();
        IndexStats indexStats = resp.getIndex(sourceShardId.getIndex().getName());
        if (indexStats == null) {
            return SequenceNumbers.UNASSIGNED_SEQ_NO;
        }
        Optional<ShardStats> filteredShardStats = Arrays.stream(indexStats.getShards())
                .filter(shardStats -> compareShardId(shardStats.getShardRouting().shardId(), sourceShardId))
                .filter(shardStats -> shardStats.getShardRouting().primary())
                .findAny();
        if (filteredShardStats.isPresent()) {
            // 异常发生时, 目标集群的globalCheckpont较老，所以这里取LocalCheckpoint .
            return filteredShardStats.get().getSeqNoStats().getLocalCheckpoint();
        } else {
            return SequenceNumbers.UNASSIGNED_SEQ_NO;
        }
    }

    private static boolean compareShardId(ShardId source, ShardId target) {
        return source.getIndexName().equals(target.getIndexName()) && source.getId() == target.getId();
    }

    /**
     * Recovery operations on follower cluster
     */
    public void indexTranslogOperations(List<Translog.Operation> operations) {
        String indexName = sourceShardId.getIndexName();
        int shardId = sourceShardId.getId();
        final TransportRequest request = new FollowerHandlerRegister.TranslogOpsRequest(indexName, shardId, operations);
        final TransportFuture<FollowerHandlerRegister.TranslogOpsResponse> future = transportService.submitRequest(
                remoteNode(),
                FollowerHandlerRegister.Actions.PROXY_TRANSLOG_OPS,
                request,
                TransportRequestOptions.EMPTY,
                FollowerHandlerRegister.TranslogOpsResponse.HANDLER
        );
        future.txGet();
    }
}
