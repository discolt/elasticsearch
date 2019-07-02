/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.vpack.xdcr.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.vpack.xdcr.action.cluster.ClusterLevelRequest;
import org.elasticsearch.vpack.xdcr.action.cluster.StopClusterSyncAction;
import org.elasticsearch.vpack.xdcr.action.index.IndexLevelRequest;
import org.elasticsearch.vpack.xdcr.action.index.StopIndexSyncAction;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestStopClusterSyncAction extends BaseRestHandler {

    public RestStopClusterSyncAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(DELETE, "/_xdcr/{repository}", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String repository = request.param("repository");
        final ClusterLevelRequest stopRequest = new ClusterLevelRequest(repository);
        return channel -> client.execute(StopClusterSyncAction.INSTANCE, stopRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "xdcr_stop_cluster_sync_action";
    }

}
