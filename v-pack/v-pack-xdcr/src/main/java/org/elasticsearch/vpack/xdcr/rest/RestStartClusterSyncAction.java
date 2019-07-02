/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.vpack.xdcr.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.vpack.xdcr.action.cluster.ClusterLevelRequest;
import org.elasticsearch.vpack.xdcr.action.cluster.StartClusterSyncAction;
import org.elasticsearch.vpack.xdcr.action.index.IndexLevelRequest;
import org.elasticsearch.vpack.xdcr.action.index.StartIndexSyncAction;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestStartClusterSyncAction extends BaseRestHandler {

    public RestStartClusterSyncAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(PUT, "/_xdcr/{repository}", this);
        controller.registerHandler(POST, "/_xdcr/{repository}", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        final String repository = request.param("repository");
        final String excludes = request.param("excludes");
        final ClusterLevelRequest startJobRequest = new ClusterLevelRequest(repository, excludes == null ? "" : excludes);
        return channel -> client.execute(StartClusterSyncAction.INSTANCE, startJobRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "xdcr_start_cluster_sync_action";
    }

}
