/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.vpack.xdcr.action.index;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.vpack.xdcr.task.index.IndexSyncTaskParams;
import org.elasticsearch.vpack.xdcr.utils.ResponseHandler;

/**
 * 创建索引同步任务
 */
public class StartIndexSyncAction extends Action<IndexLevelRequest, AcknowledgedResponse, IndexLevelRequest.Builder> {

    public static final StartIndexSyncAction INSTANCE = new StartIndexSyncAction();
    public static final String NAME = "cluster:admin/vpack/xdcr/index/sync/start";

    private StartIndexSyncAction() {
        super(NAME);
    }

    @Override
    public AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    public IndexLevelRequest.Builder newRequestBuilder(ElasticsearchClient client) {
        return new IndexLevelRequest.Builder(client, INSTANCE, new IndexLevelRequest());
    }

    /**
     * Transport
     */
    public static class Transport extends TransportMasterNodeAction<IndexLevelRequest, AcknowledgedResponse> {

        private final PersistentTasksService persistentTasksService;

        @Inject
        public Transport(Settings settings, TransportService transportService, ThreadPool threadPool,
                         ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                         ClusterService clusterService, PersistentTasksService persistentTasksService, Client client) {
            super(settings, StartIndexSyncAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, IndexLevelRequest::new);
            this.persistentTasksService = persistentTasksService;
        }


        @Override
        protected void masterOperation(IndexLevelRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener) throws Exception {
            String taskId = request.getTaskId();
            checkSoftDeleteEnable(state, request.index());
            checkRemoteClusterRegisted(state, request.repository());
            final ResponseHandler handler = new ResponseHandler(listener);
            persistentTasksService.sendStartRequest(
                    taskId, IndexSyncTaskParams.NAME,
                    new IndexSyncTaskParams(request.repository(), request.index()), handler.getActionListener());
        }

        @Override
        protected ClusterBlockException checkBlock(IndexLevelRequest request, ClusterState state) {
            return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.index());
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected AcknowledgedResponse newResponse() {
            return new AcknowledgedResponse();
        }

        // 检查索引是否开启软删除
        private void checkSoftDeleteEnable(ClusterState state, String index) {
            IndexMetaData indexMetaData = state.metaData().index(index);
            if (indexMetaData == null) {
                throw new IllegalArgumentException("index [" + index + "] not found.");
            }
            if (IndexSettings.INDEX_SOFT_DELETES_SETTING.get(indexMetaData.getSettings()) == false) {
                throw new IllegalArgumentException("leader index [" + index + "] does not have soft deletes enabled.");
            }
        }

        // 检查远程集群是否已存在
        private void checkRemoteClusterRegisted(ClusterState state, String repository) {
            String val = state.metaData().settings().get("cluster.remote." + repository + ".seeds");
            if (val == null || val.trim().length() == 0) {
                throw new IllegalArgumentException("remote cluster [" + repository + "] does not registed.");
            }
        }
    }

}
