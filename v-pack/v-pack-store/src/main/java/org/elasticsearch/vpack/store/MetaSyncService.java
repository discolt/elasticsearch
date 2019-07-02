package org.elasticsearch.vpack.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.elasticsearch.threadpool.ThreadPool;

import static org.elasticsearch.vpack.store.StoreSettings.INDEX_STORE_REMOTE_SETTINGS;

/**
 * 元数据同步
 */
public class MetaSyncService implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(MetaSyncService.class);
    private final Client client;
    private final ThreadPool threadPool;

    public MetaSyncService(Client client, ThreadPool threadPool) {
        this.client = client;
        this.threadPool = threadPool;
    }

    @Override
    public void beforeIndexAddedToCluster(Index index, Settings indexSettings) {
        String remoteStore = INDEX_STORE_REMOTE_SETTINGS.get(indexSettings);
        if (!Strings.isNullOrEmpty(remoteStore)) {
            CreateIndexRequest request = buildCreateRequest(index, indexSettings);
            client.getRemoteClusterClient(remoteStore).admin().indices().create(request).actionGet();
        }
    }

    @Override
    public void afterIndexRemoved(Index index, IndexSettings indexSettings, IndexRemovalReason reason) {
        String remoteStore = indexSettings.getValue(INDEX_STORE_REMOTE_SETTINGS);
        if (!Strings.isNullOrEmpty(remoteStore) && IndexRemovalReason.DELETED.equals(reason)) {
            threadPool.generic().submit(new AbstractRunnable() {
                @Override
                protected void doRun() {
                    // 若删除失败注册持久化任务后台执行 ？
                    client.getRemoteClusterClient(remoteStore).admin().indices().prepareDelete(index.getName()).get();
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn("failed remove remote store index {}, cause {}", index, e);
                }
            });
        }
    }

    /**
     * Create Remote Index
     * TODO use UUID?
     * @param index
     * @param indexSettings
     * @returnT
     */
    private CreateIndexRequest buildCreateRequest(Index index, Settings indexSettings) {
        int shards = indexSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1);
        int replicas = indexSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0);
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(index.getName());
        Settings settings = Settings.builder()
                .put("index.number_of_shards", shards)
                .put("index.number_of_replicas", Math.min(1, replicas))
                .put("index.refresh_interval", "60s")
                .put("index.translog.durability", "async")
                .put("codec", "best_compression")
                .build();
        createIndexRequest.settings(settings);
        createIndexRequest.mapping("_doc", INDEX_STORE_MAPPING, XContentType.YAML);
        return createIndexRequest;
    }

    private static String INDEX_STORE_MAPPING =
            "properties:\n" +
                    "  source:\n" +
                    "    type: keyword\n" +
                    "    index: false\n" +
                    "    doc_values: false\n";

}
