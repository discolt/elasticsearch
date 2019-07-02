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

package org.elasticsearch.vpack.xdcr.action.stats;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.vpack.xdcr.core.ChunkSentTrackerTask;
import org.elasticsearch.vpack.xdcr.core.TaskService;

import java.io.IOException;
import java.util.List;

public class TransportNodesXRStatsAction extends TransportNodesAction<NodesXRStatsRequest, NodesXRStatsResponse, TransportNodesXRStatsAction.NodeXRStatsRequest, NodeXRStats> {


    private final TaskService taskService;

    @Inject
    public TransportNodesXRStatsAction(Settings settings, ThreadPool threadPool,
                                       ClusterService clusterService, TransportService transportService,
                                       TaskService taskService, ActionFilters actionFilters,
                                       IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, NodesXRStatsAction.NAME, threadPool, clusterService, transportService, actionFilters,
                indexNameExpressionResolver, NodesXRStatsRequest::new, NodeXRStatsRequest::new, ThreadPool.Names.MANAGEMENT, NodeXRStats.class);
        this.taskService = taskService;
    }

    @Override
    protected NodesXRStatsResponse newResponse(NodesXRStatsRequest request, List<NodeXRStats> nodeXRStats, List<FailedNodeException> failures) {
        return new NodesXRStatsResponse(clusterService.getClusterName(), nodeXRStats, failures);
    }

    @Override
    protected NodeXRStatsRequest newNodeRequest(String nodeId, NodesXRStatsRequest request) {
        return new NodeXRStatsRequest(nodeId, request);
    }

    @Override
    protected NodeXRStats newNodeResponse() {
        return new NodeXRStats();
    }

    @Override
    protected NodeXRStats nodeOperation(NodeXRStatsRequest request) {
        boolean filterBlocked = ChunkSentTrackerTask.STATUS_BLOCKED.equals(request.status());
        return taskService.stats(filterBlocked);
    }


    public static class NodeXRStatsRequest extends BaseNodeRequest {

        NodesXRStatsRequest request;

        public NodeXRStatsRequest() {
        }

        public String status() {
            return request.status();
        }

        public NodeXRStatsRequest(String nodeId, NodesXRStatsRequest request) {
            super(nodeId);
            this.request = request;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            request = new NodesXRStatsRequest();
            request.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
