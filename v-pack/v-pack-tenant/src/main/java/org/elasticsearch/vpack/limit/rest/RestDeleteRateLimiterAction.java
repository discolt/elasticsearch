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

package org.elasticsearch.vpack.limit.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.vpack.limit.action.delete.DeleteRateLimiterAction;
import org.elasticsearch.vpack.limit.action.delete.DeleteRateLimiterRequest;
import org.elasticsearch.vpack.limit.action.put.PutRateLimiterAction;
import org.elasticsearch.vpack.limit.action.put.PutRateLimiterRequest;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;


public class RestDeleteRateLimiterAction extends BaseRestHandler {

    public RestDeleteRateLimiterAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(DELETE, "/_limit/{tenant}", this);
    }

    @Override
    public String getName() {
        return "tenant_delete_ratelimit_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        DeleteRateLimiterRequest request = new DeleteRateLimiterRequest(restRequest.param("tenant"));
        request.masterNodeTimeout(restRequest.paramAsTime("master_timeout", request.masterNodeTimeout()));
        request.timeout(restRequest.paramAsTime("timeout", request.timeout()));
        return channel -> client.execute(DeleteRateLimiterAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

}
