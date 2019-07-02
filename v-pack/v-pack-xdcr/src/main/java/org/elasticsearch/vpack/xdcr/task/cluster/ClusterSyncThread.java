package org.elasticsearch.vpack.xdcr.task.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.vpack.xdcr.action.index.IndexLevelRequest;
import org.elasticsearch.vpack.xdcr.action.index.StartIndexSyncAction;
import org.elasticsearch.vpack.xdcr.action.index.StopIndexSyncAction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;

public class ClusterSyncThread implements Runnable {

    private static final Logger logger = LogManager.getLogger(ClusterSyncThread.class);

    private final Client client;
    private final String repository;
    private final String[] exclusions;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;


    ClusterSyncThread(ClusterService clusterService, Client client, String repository, String exclusions) {
        this.client = client;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = new IndexNameExpressionResolver(client.settings());
        this.repository = repository;
        this.exclusions = Strings.splitStringByCommaToArray(exclusions);
    }

    @Override
    public void run() {
        ClusterState state = clusterService.state();

        List<String> excloudIndices = new ArrayList<>();
        if (exclusions.length == 0) {
            excloudIndices = Collections.EMPTY_LIST;
        } else {
            try {
                String[] resolveredIndices = indexNameExpressionResolver.concreteIndexNames(state, new ExclusionsIndicesRequest());
                for (String resolverIndex : resolveredIndices) {
                    excloudIndices.add(resolverIndex);
                }
            } catch (Exception e) {
                // allow not such index exception
            }
        }

        // filter index.soft_deleted enabled indices
        ImmutableOpenMap<String, Settings> indicesSettings = client.admin().indices().prepareGetSettings().get().getIndexToSettings();
        List<String> indices = StreamSupport.stream(indicesSettings.spliterator(), false)
                .filter(s -> INDEX_SOFT_DELETES_SETTING.get(s.value) == true)
                .map(c -> c.key)
                .collect(Collectors.toList());

        PersistentTasksCustomMetaData.Builder builder = PersistentTasksCustomMetaData.builder(state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE));
        for (String index : indices) {
            String taskId = IndexLevelRequest.taskId(repository, index);
            if (builder.hasTask(taskId)) {
                // 如果需要被排除
                if (excloudIndices.contains(index)) {
                    // 停止任务
                    client.execute(StopIndexSyncAction.INSTANCE, new IndexLevelRequest(repository, index)).actionGet();
                }
            } else {
                // 如果不在排除范围内
                if (!excloudIndices.contains(index)) {
                    // 启动任务
                    client.execute(StartIndexSyncAction.INSTANCE, new IndexLevelRequest(repository, index)).actionGet();
                }
            }
        }
    }

    private class ExclusionsIndicesRequest implements IndicesRequest {

        @Override
        public String[] indices() {
            return exclusions;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.lenientExpandOpen();
        }
    }

}
