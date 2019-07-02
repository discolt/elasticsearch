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

package org.elasticsearch.vpack.limit.action.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
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
import org.elasticsearch.vpack.limit.RateLimiterService;

public class PutRateLimiterTransportAction extends TransportMasterNodeAction<PutRateLimiterRequest, AcknowledgedResponse> {

    private final RateLimiterService ratelimiterService;

    @Inject
    public PutRateLimiterTransportAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                         TransportService transportService, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver, RateLimiterService ratelimiterService,
                                         TransportNodesInfoAction nodesInfoAction) {
        super(settings, PutRateLimiterAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, PutRateLimiterRequest::new);
        this.ratelimiterService = ratelimiterService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    protected void masterOperation(PutRateLimiterRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener) throws Exception{
        ratelimiterService.putLimiter(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(PutRateLimiterRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

}
