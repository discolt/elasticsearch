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

package org.elasticsearch.vpack.adaptz.bulk;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.vpack.adaptz.Adapts;
import org.elasticsearch.vpack.adaptz.bulk.action.IndicesAppendOnlyExistsAction;
import org.elasticsearch.vpack.adaptz.bulk.action.IndicesAppendOnlyExistsRequest;
import org.elasticsearch.vpack.adaptz.bulk.action.IndicesAppendOnlyExistsResponse;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.vpack.adaptz.bulk.Constants.*;

public class RestBulkProxyAction extends BaseRestHandler {
    private static final DeprecationLogger DEPRECATION_LOGGER =
            new DeprecationLogger(Loggers.getLogger(org.elasticsearch.rest.action.document.RestBulkAction.class));

    private final boolean allowExplicitIndex;

    public RestBulkProxyAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(POST, "/_zbulk", this);
        controller.registerHandler(PUT, "/_zbulk", this);
        controller.registerHandler(POST, "/{index}/_zbulk", this);
        controller.registerHandler(PUT, "/{index}/_zbulk", this);
        controller.registerHandler(POST, "/{index}/{type}/_zbulk", this);
        controller.registerHandler(PUT, "/{index}/{type}/_zbulk", this);
        this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
    }

    @Override
    public String getName() {
        return "zbulk_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        // 根据header获取appname
        String appName = Adapts.getAppName(request);
        BulkRequestProxy bulkRequestProxy = new BulkRequestProxy();
        // 转换成真实索引
        String defaultIndex = Adapts.toConcreteIndex(appName, request.param("index"));
        String defaultType = request.param("type");
        String defaultRouting = request.param("routing");
        FetchSourceContext defaultFetchSourceContext = FetchSourceContext.parseFromRestRequest(request);
        String fieldsParam = request.param("fields");
        if (fieldsParam != null) {
            DEPRECATION_LOGGER.deprecated("Deprecated field [fields] used, expected [_source] instead");
        }
        String[] defaultFields = fieldsParam != null ? Strings.commaDelimitedListToStringArray(fieldsParam) : null;
        String defaultPipeline = Adapts.toConcreteIndex(appName, request.param("pipeline"));
        String waitForActiveShards = request.param("wait_for_active_shards");
        if (waitForActiveShards != null) {
            bulkRequestProxy.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
        }
        bulkRequestProxy.timeout(request.paramAsTime("timeout", BulkRequestProxy.DEFAULT_TIMEOUT));
        bulkRequestProxy.setRefreshPolicy(request.param("refresh"));
        bulkRequestProxy.add(request.requiredContent(), appName, defaultIndex, defaultType, defaultRouting, defaultFields,
                defaultFetchSourceContext, defaultPipeline, null, allowExplicitIndex, request.getXContentType());
        // 校验
        Adapts.authAppIndices(client, appName, bulkRequestProxy.indices());

        return channel -> client.bulk(toBulkRequest(bulkRequestProxy, client), new RestStatusToXContentListener<>(channel));
    }

    public BulkRequest toBulkRequest(BulkRequestProxy wrapper, NodeClient client) {

        IndicesAppendOnlyExistsRequest existsRequest = new IndicesAppendOnlyExistsRequest(wrapper.indices());
        IndicesAppendOnlyExistsResponse response = client.admin().indices().execute(IndicesAppendOnlyExistsAction.INSTANCE, existsRequest).actionGet();
        Map<String, Object> existsMap = response.existsMap();

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(wrapper.getRefreshPolicy());
        bulkRequest.setParentTask(wrapper.getParentTask());
        bulkRequest.waitForActiveShards(wrapper.waitForActiveShards());
        bulkRequest.remoteAddress(wrapper.remoteAddress());
        bulkRequest.timeout(wrapper.timeout());

        wrapper.requests.forEach(e -> {
            if ((Boolean) existsMap.get(e.index())) {
                if (e instanceof DeleteRequest) {
                    ((DeleteRequest) e).index(APPEND_WRITE_PREFIX + e.index());
                }
                if (e instanceof UpdateRequest) {
                    ((UpdateRequest) e).index(APPEND_WRITE_PREFIX + e.index());
                }
                if (e instanceof IndexRequest) {
                    ((IndexRequest) e).index(APPEND_WRITE_PREFIX + e.index());
                }
            }
            bulkRequest.add(e);

        });
        return bulkRequest;
    }


    @Override
    public boolean supportsContentStream() {
        return true;
    }
}
