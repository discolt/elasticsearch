/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.vpack.xdcr.action.cluster;

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
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.vpack.xdcr.task.cluster.ClusterSyncTaskParams;
import org.elasticsearch.vpack.xdcr.utils.ResponseHandler;

import static org.elasticsearch.vpack.xdcr.utils.Actions.always;
import static org.elasticsearch.vpack.xdcr.utils.Utils.hasTask;
import static org.elasticsearch.vpack.xdcr.utils.Utils.remoteClusterPrepared;

/**
 * 创建同步任务
 */
public class StartClusterSyncAction extends Action<ClusterLevelRequest, AcknowledgedResponse, ClusterLevelRequest.Builder> {

    public static final StartClusterSyncAction INSTANCE = new StartClusterSyncAction();
    public static final String NAME = "cluster:admin/vpack/xdcr/cluster/sync/start";

    private StartClusterSyncAction() {
        super(NAME);
    }

    @Override
    public AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    public ClusterLevelRequest.Builder newRequestBuilder(ElasticsearchClient client) {
        return new ClusterLevelRequest.Builder(client, INSTANCE, new ClusterLevelRequest());
    }

    /**
     * Transport
     */
    public static class Transport extends TransportMasterNodeAction<ClusterLevelRequest, AcknowledgedResponse> {

        private final PersistentTasksService persistentTasksService;
        private final Client client;

        @Inject
        public Transport(Settings settings, TransportService transportService, ThreadPool threadPool,
                         ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                         ClusterService clusterService, PersistentTasksService persistentTasksService, Client client) {
            super(settings, StartClusterSyncAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, ClusterLevelRequest::new);
            this.persistentTasksService = persistentTasksService;
            this.client = client;
        }

        @Override
        protected void masterOperation(ClusterLevelRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener) throws Exception {
            // 校验远程集群是否可用 backup
            if (!remoteClusterPrepared(client, request.repository())) {
                throw new RemoteTransportException("error while communicating with remote cluster [" + request.repository() + "]", null);
            }
            // restart task
            final ResponseHandler handler = new ResponseHandler(listener);
            ClusterSyncTaskParams taskParams = new ClusterSyncTaskParams(request.repository(), request.exclusions());
            if (hasTask(request.getTaskId(), state)) {
                persistentTasksService.sendRemoveRequest(request.getTaskId(), always(r -> {
                    persistentTasksService.sendStartRequest(
                            request.getTaskId(), ClusterSyncTaskParams.NAME,
                            taskParams, handler.getActionListener());
                }));
            } else {
                persistentTasksService.sendStartRequest(
                        request.getTaskId(), ClusterSyncTaskParams.NAME,
                        taskParams, handler.getActionListener());
            }
        }

        @Override
        protected ClusterBlockException checkBlock(ClusterLevelRequest request, ClusterState state) {
            return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.repository());
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
