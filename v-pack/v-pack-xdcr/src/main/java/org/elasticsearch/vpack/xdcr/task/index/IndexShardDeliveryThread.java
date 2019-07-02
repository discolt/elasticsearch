package org.elasticsearch.vpack.xdcr.task.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.vpack.xdcr.action.index.ShardDeliveryAction;

import java.util.Arrays;

import static org.elasticsearch.vpack.xdcr.utils.Utils.sleep;

public class IndexShardDeliveryThread implements Runnable {

    private static final Logger logger = LogManager.getLogger(IndexShardDeliveryThread.class);
    private final ClusterService clusterService;
    private final Client client;
    private final IndexSyncTaskParams params;
    private final int shard;

    IndexShardDeliveryThread(ClusterService clusterService, Client client, IndexSyncTaskParams params, int shard) {
        this.clusterService = clusterService;
        this.client = client;
        this.params = params;
        this.shard = shard;
    }

    @Override
    public void run() {
        Client remote = client.getRemoteClusterClient(params.repository());
        String index = params.index();

        // 本地集群索引状态
        ShardStats[] sourceSS = client.admin().indices().prepareStats(index).get().getShards();
        ShardStats sourceShardStats = Arrays.stream(sourceSS)
                .filter(shardStats -> shardStats.getShardRouting().primary() && shardStats.getShardRouting().shardId().getId() == shard)
                .findAny()
                .orElse(null);
        if (sourceShardStats == null) {
            logger.warn("waiting leader shard ready " + index + "[" + shard + "]");
            sleep(5);
            return;
        }

        // wait index start
        ShardStats[] destSS;
        try {
            destSS = remote.admin().indices().prepareStats(index).get().getShards();
        } catch (Exception e) {
            logger.warn("waiting remote index {} ready , cause {}", params, e.getMessage());
            sleep(5);
            return;
        }
        // wait to shard start
        ShardStats destShardStats = Arrays.stream(destSS)
                .filter(shardStats -> shardStats.getShardRouting().primary() && shardStats.getShardRouting().shardId().getId() == shard)
                .findAny()
                .orElse(null);
        if (destShardStats == null) {
            logger.warn("waiting remote index shard {} ready ", params);
            sleep(5);
            return;
        }

        SeqNoStats sourceSeqNoStats = sourceShardStats.getSeqNoStats();
        SeqNoStats destShardSeqNoStats = destShardStats.getSeqNoStats();

        // 发送数据
        if (sourceSeqNoStats.getMaxSeqNo() > destShardSeqNoStats.getMaxSeqNo()) {
            ShardId shardId = clusterService.state().routingTable().index(params.index()).shard(shard).primaryShard().shardId();
            ShardDeliveryAction.Request readRequest = new ShardDeliveryAction.Request(shardId, params.repository());
            readRequest.setMaxOperationCount(Integer.MAX_VALUE);
            readRequest.setFromSeqNo(destShardSeqNoStats.getMaxSeqNo() + 1);
            client.execute(ShardDeliveryAction.INSTANCE, readRequest).actionGet();
        } else {
            sleep(5);
        }
    }
}

