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

package org.elasticsearch.vpack.xdcr.action.feed.delete;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

/**
 * Creates a new index
 */
public class RestDeleteFeedAction extends BaseRestHandler {
    public RestDeleteFeedAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(DELETE, "/_xdcr/{repository}/{index}", this);
    }

    @Override
    public String getName() {
        return "delete_xdcr_feed_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        DeleteFeedRequest deleteFeedRequest = new DeleteFeedRequest(request.param("repository"), request.param("index"));
        deleteFeedRequest.masterNodeTimeout(request.paramAsTime("master_timeout", deleteFeedRequest.masterNodeTimeout()));
        return channel -> client.execute(DeleteFeedAction.INSTANCE, deleteFeedRequest, new RestToXContentListener(channel));
    }
}
