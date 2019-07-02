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

package org.elasticsearch.vpack.adaptz.mget;

import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.vpack.adaptz.Adapts;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestMultiGetProxyAction extends BaseRestHandler {

    private final boolean allowExplicitIndex;

    public RestMultiGetProxyAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_zmget", this);
        controller.registerHandler(POST, "/_zmget", this);
        controller.registerHandler(GET, "/{index}/_zmget", this);
        controller.registerHandler(POST, "/{index}/_zmget", this);
        controller.registerHandler(GET, "/{index}/{type}/_zmget", this);
        controller.registerHandler(POST, "/{index}/{type}/_zmget", this);

        this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
    }

    @Override
    public String getName() {
        return "document_zmget_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String appName = Adapts.getAppName(request);
        String defaultIndex = request.param("index");
        defaultIndex = Adapts.toConcreteIndex(appName, defaultIndex);
        MultiGetRequestProxy multiGetRequestProxy = new MultiGetRequestProxy();
        multiGetRequestProxy.refresh(request.paramAsBoolean("refresh", multiGetRequestProxy.refresh()));
        multiGetRequestProxy.preference(request.param("preference"));
        multiGetRequestProxy.realtime(request.paramAsBoolean("realtime", multiGetRequestProxy.realtime()));
        if (request.param("fields") != null) {
            throw new IllegalArgumentException("The parameter [fields] is no longer supported, " +
                    "please use [stored_fields] to retrieve stored fields or _source filtering if the field is not stored");
        }
        String[] sFields = null;
        String sField = request.param("stored_fields");
        if (sField != null) {
            sFields = Strings.splitStringByCommaToArray(sField);
        }

        FetchSourceContext defaultFetchSource = FetchSourceContext.parseFromRestRequest(request);
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            multiGetRequestProxy.add(appName, defaultIndex, request.param("type"), sFields, defaultFetchSource,
                    request.param("routing"), parser, allowExplicitIndex);
        }

        // 校验
        Set<String> indices = new HashSet();
        for (MultiGetRequestProxy.Item item : multiGetRequestProxy.items) {
            indices.add(item.index());
        }
        Adapts.authAppIndices(client, appName, indices);
        return channel -> client.multiGet(toMultiGetRequest(multiGetRequestProxy), new RestToXContentListener<>(channel));
    }

    public static MultiGetRequest toMultiGetRequest(MultiGetRequestProxy wrapper) {
        MultiGetRequest request = new MultiGetRequest();
        request.setParentTask(request.getParentTask());
        request.preference(request.preference());
        request.realtime(request.realtime());
        for (MultiGetRequestProxy.Item item : wrapper.items) {
            request.add(new MultiGetRequest.Item(item.index(), item.type(), item.id()));
        }
        return request;
    }
}
