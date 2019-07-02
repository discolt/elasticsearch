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
import org.elasticsearch.vpack.xdcr.action.stats.StatsAction;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestStatsAction extends BaseRestHandler {

    public RestStatsAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_xdcr/stats", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        return channel -> client.execute(StatsAction.INSTANCE, new StatsAction.Request(), new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "xdcr_stats_action";
    }

}
