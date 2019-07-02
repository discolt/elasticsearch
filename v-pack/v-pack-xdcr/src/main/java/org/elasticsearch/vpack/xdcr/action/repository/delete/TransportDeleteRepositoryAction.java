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

package org.elasticsearch.vpack.xdcr.action.repository.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for unregister repository operation
 */
public class TransportDeleteRepositoryAction extends TransportMasterNodeAction<DeleteRepositoryRequest, DeleteRepositoryResponse> {

    private final TransportClusterUpdateSettingsAction updateSettingsAction;

    @Inject
    public TransportDeleteRepositoryAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                           TransportClusterUpdateSettingsAction updateSettingsAction,
                                           ThreadPool threadPool, ActionFilters actionFilters,
                                           IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, DeleteRepositoryAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, DeleteRepositoryRequest::new);
        this.updateSettingsAction = updateSettingsAction;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected DeleteRepositoryResponse newResponse() {
        return new DeleteRepositoryResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteRepositoryRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(final DeleteRepositoryRequest request, ClusterState state, final ActionListener<DeleteRepositoryResponse> listener) throws IOException {
        throw new UnsupportedOperationException("Unsupport delete xdcr repository");
    }

//    private ClusterUpdateSettingsRequest getClusterUpdateSettingsRequest(DeleteRepositoryRequest request) {
//        String name = request.name();
//        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
//        Map<String, Object> map = new HashMap();
//        map.put("search.remote." + name + ".seeds", null);
//        updateSettingsRequest.transientSettings(map);
//        return updateSettingsRequest;
//    }
}
