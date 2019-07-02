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
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.vpack.xdcr.utils.ResponseHandler;

/**
 * 停止索引同步任务
 */
public class StopIndexSyncAction extends Action<IndexLevelRequest, AcknowledgedResponse, IndexLevelRequest.Builder> {

    public static final StopIndexSyncAction INSTANCE = new StopIndexSyncAction();
    public static final String NAME = "cluster:admin/vpack/xdcr/index/sync/stop";

    private StopIndexSyncAction() {
        super(NAME);
    }

    @Override
    public IndexLevelRequest.Builder newRequestBuilder(ElasticsearchClient client) {
        return new IndexLevelRequest.Builder(client, INSTANCE, new IndexLevelRequest());
    }

    @Override
    public AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
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
            super(settings, StopIndexSyncAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, IndexLevelRequest::new);
            this.persistentTasksService = persistentTasksService;
        }


        @Override
        protected void masterOperation(IndexLevelRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener) throws Exception {
            final ResponseHandler responseHandler = new ResponseHandler(listener);
            String taskId = request.getTaskId();
            persistentTasksService.sendRemoveRequest(taskId, responseHandler.getActionListener());
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

    }
}
