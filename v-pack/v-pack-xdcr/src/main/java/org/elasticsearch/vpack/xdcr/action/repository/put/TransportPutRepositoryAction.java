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

package org.elasticsearch.vpack.xdcr.action.repository.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Transport action for register repository operation
 */
public class TransportPutRepositoryAction extends TransportMasterNodeAction<PutRepositoryRequest, PutRepositoryResponse> {

    private final TransportClusterUpdateSettingsAction updateSettingsAction;
    private final TransportService transportService;

    @Inject
    public TransportPutRepositoryAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                        TransportClusterUpdateSettingsAction updateSettingsAction,
                                        ThreadPool threadPool, ActionFilters actionFilters,
                                        IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, PutRepositoryAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, PutRepositoryRequest::new);
        this.transportService = transportService;
        this.updateSettingsAction = updateSettingsAction;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutRepositoryResponse newResponse() {
        return new PutRepositoryResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(PutRepositoryRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(final PutRepositoryRequest request, ClusterState state, final ActionListener<PutRepositoryResponse> listener) {
        ClusterUpdateSettingsRequest updateSettingsRequest = getClusterUpdateSettingsRequest(request);
        updateSettingsAction.execute(updateSettingsRequest, new ActionListener<ClusterUpdateSettingsResponse>() {
            @Override
            public void onResponse(ClusterUpdateSettingsResponse clusterUpdateSettingsResponse) {

                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                }

                transportService.getRemoteClusterService().ensureConnected(request.name(), new ActionListener<Void>() {
                    @Override
                    public void onResponse(Void aVoid) {
                        listener.onResponse(new PutRepositoryResponse(true));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                });

            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private ClusterUpdateSettingsRequest getClusterUpdateSettingsRequest(PutRepositoryRequest request) {
        String name = request.name();
        List<String> seeds = request.settings().getAsList("seeds");
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        Map<String, Object> map = new HashMap<>();
        map.put("search.remote." + name + ".seeds", seeds);
        updateSettingsRequest.transientSettings(map);
        return updateSettingsRequest;
    }
}
