package org.elasticsearch.vpack.xdcr.action.stats;

import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.vpack.xdcr.task.index.IndexSyncTaskParams;

import java.util.*;
import java.util.function.BiFunction;

import static org.elasticsearch.vpack.xdcr.utils.Utils.remoteClusterPrepared;

/**
 * Collector for XDCRStats
 */
public class StatsCollector {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(2);
    private static final long SEQNO_UNKOWN = -999;
    private Client client;
    private RepositoryIndices container;


    public StatsCollector(Client client) {
        this.client = client;
        this.container = new RepositoryIndices();
    }

    public synchronized List<Stats> collect() {
        List<Stats> stats = new ArrayList<>();
        container = new RepositoryIndices();
        ClusterState clusterState = client.admin().cluster().prepareState().get(TIMEOUT).getState();
        PersistentTasksCustomMetaData persistentTasks = clusterState.metaData().custom(PersistentTasksCustomMetaData.TYPE);
        if (persistentTasks == null) {
            return Collections.EMPTY_LIST;
        }

        persistentTasks.tasks().stream()
                .filter(t -> t.getTaskName().equals(IndexSyncTaskParams.NAME))
                .forEach(task -> {
                    IndexSyncTaskParams params = (IndexSyncTaskParams) task.getParams();
                    container.add(params.repository(), params.index());
                });
        if (container.isEmpty()) {
            return stats;
        }

        // 获取远程集群ShardsMaxSeqNo
        BiFunction<IndexStats, Integer, Long> remoteSeqNoGetter = (remoteIndexStats, shard) -> {
            if (remoteIndexStats == null) {
                return SEQNO_UNKOWN;
            }
            Optional<ShardStats> optional = Arrays.stream(remoteIndexStats.getShards())
                    .filter(e -> e.getShardRouting().primary() && e.getShardRouting().shardId().id() == shard)
                    .findFirst();
            return optional.isPresent() ?
                    optional.get().getSeqNoStats() != null ? optional.get().getSeqNoStats().getMaxSeqNo() : SEQNO_UNKOWN : SEQNO_UNKOWN;
        };

        // 迭代每个远程集群，获取本地及远程shard的maxSeqNo
        container.getRepositoryIndices().forEach((repository, indexSet) -> {
            String[] indices = indexSet.toArray(new String[indexSet.size()]);
            if (indexSet.isEmpty()) {
                return;
            }
            boolean connected = remoteClusterPrepared(client, repository);
            IndicesStatsResponse localIndicesStats = client.admin().indices().prepareStats(indices).get(TIMEOUT);
            if (!connected) {
                localIndicesStats.getIndices().forEach((index, indexStats) -> Arrays.stream(indexStats.getShards())
                        .filter(e -> e.getShardRouting().primary())
                        .forEach(e -> stats.add(new Stats(index, repository, e.getShardRouting().id(), e.getSeqNoStats().getMaxSeqNo(), SEQNO_UNKOWN))));

            } else {
                IndicesStatsResponse remoteIndicesStats = client.getRemoteClusterClient(repository).admin().indices().prepareStats(indices).get(TIMEOUT);
                localIndicesStats.getIndices().forEach(
                        (index, indexStats) -> Arrays.stream(indexStats.getShards())
                                .filter(e -> e.getShardRouting().primary())
                                .forEach(e -> stats.add(
                                        new Stats(index,
                                                repository,
                                                e.getShardRouting().id(),
                                                e.getSeqNoStats().getMaxSeqNo(),
                                                remoteSeqNoGetter.apply(remoteIndicesStats.getIndex(index), e.getShardRouting().id())
                                        )))
                );
            }

        });

        return stats;
    }


    private static class RepositoryIndices {
        private Map<String, Set<String>> groupMap = new HashMap<>();

        public void add(String repository, String index) {
            if (!groupMap.containsKey(repository)) {
                groupMap.put(repository, new HashSet<>());
            }
            groupMap.get(repository).add(index);
        }

        public Map<String, Set<String>> getRepositoryIndices() {
            return groupMap;
        }

        public boolean isEmpty() {
            return groupMap.isEmpty();
        }
    }
}
