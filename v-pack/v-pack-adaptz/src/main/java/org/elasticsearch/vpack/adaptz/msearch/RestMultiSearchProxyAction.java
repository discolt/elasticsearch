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

package org.elasticsearch.vpack.adaptz.msearch;

import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.vpack.adaptz.Adapts;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestMultiSearchProxyAction extends BaseRestHandler {

    private static final Set<String> RESPONSE_PARAMS = Collections.singleton(RestSearchAction.TYPED_KEYS_PARAM);

    private final boolean allowExplicitIndex;

    public RestMultiSearchProxyAction(Settings settings, RestController controller) {
        super(settings);

        controller.registerHandler(GET, "/_zmsearch", this);
        controller.registerHandler(POST, "/_zmsearch", this);
        controller.registerHandler(GET, "/{index}/_zmsearch", this);
        controller.registerHandler(POST, "/{index}/_zmsearch", this);
        controller.registerHandler(GET, "/{index}/{type}/_zmsearch", this);
        controller.registerHandler(POST, "/{index}/{type}/_zmsearch", this);

        this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
    }

    @Override
    public String getName() {
        return "zmsearch_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String appName = Adapts.getAppName(request);
        MultiSearchRequest multiSearchRequest = parseRequest(request, allowExplicitIndex);
        // rewrite to user index
        Set<String> indexSet = new HashSet<>();
        MultiSearchRequest replacedRequest = new MultiSearchRequest();
        replacedRequest.setParentTask(multiSearchRequest.getParentTask());
        replacedRequest.indicesOptions(multiSearchRequest.indicesOptions());
        if (multiSearchRequest.maxConcurrentSearchRequests() > 0) {
            replacedRequest.maxConcurrentSearchRequests(multiSearchRequest.maxConcurrentSearchRequests());
        }
        for (SearchRequest searchRequest : multiSearchRequest.requests()) {
            String[] indices = searchRequest.indices();
            // 如果index为空则为应用别名， 否则转换真实索引名
            if (indices == null || indices.length == 0) {
                searchRequest.indices(appName);
            } else {
                searchRequest.indices(Adapts.concreteIndices(appName, searchRequest.indices()));
            }
            // 带校验的真实索引
            for (String index : searchRequest.indices()) {
                if (!index.equals(appName)) {
                    indexSet.add(index);
                }
            }
            replacedRequest.add(searchRequest);
        }
        Adapts.authAppIndices(client, appName, indexSet);
        return channel -> client.multiSearch(replacedRequest, new RestToXContentListener<>(channel));
    }

    /**
     * Parses a {@link RestRequest} body and returns a {@link MultiSearchRequest}
     */
    public static MultiSearchRequest parseRequest(RestRequest restRequest, boolean allowExplicitIndex) throws IOException {
        MultiSearchRequest multiRequest = new MultiSearchRequest();
        if (restRequest.hasParam("max_concurrent_searches")) {
            multiRequest.maxConcurrentSearchRequests(restRequest.paramAsInt("max_concurrent_searches", 0));
        }

        int preFilterShardSize = restRequest.paramAsInt("pre_filter_shard_size", SearchRequest.DEFAULT_PRE_FILTER_SHARD_SIZE);


        parseMultiLineRequest(restRequest, multiRequest.indicesOptions(), allowExplicitIndex, (searchRequest, parser) -> {
            searchRequest.source(SearchSourceBuilder.fromXContent(parser, false));
            multiRequest.add(searchRequest);
        });
        List<SearchRequest> requests = multiRequest.requests();
        preFilterShardSize = Math.max(1, preFilterShardSize / (requests.size() + 1));
        for (SearchRequest request : requests) {
            // preserve if it's set on the request
            request.setPreFilterShardSize(Math.min(preFilterShardSize, request.getPreFilterShardSize()));
        }
        return multiRequest;
    }

    /**
     * Parses a multi-line {@link RestRequest} body, instantiating a {@link SearchRequest} for each line and applying the given consumer.
     */
    public static void parseMultiLineRequest(RestRequest request, IndicesOptions indicesOptions, boolean allowExplicitIndex,
                                             CheckedBiConsumer<SearchRequest, XContentParser, IOException> consumer) throws IOException {

        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        String[] types = Strings.splitStringByCommaToArray(request.param("type"));
        String searchType = request.param("search_type");
        String routing = request.param("routing");

        final Tuple<XContentType, BytesReference> sourceTuple = request.contentOrSourceParam();
        final XContent xContent = sourceTuple.v1().xContent();
        final BytesReference data = sourceTuple.v2();
        MultiSearchRequest.readMultiLineFormat(data, xContent, consumer, indices, indicesOptions, types, routing,
                searchType, request.getXContentRegistry(), allowExplicitIndex);
    }

    @Override
    public boolean supportsContentStream() {
        return true;
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }
}
