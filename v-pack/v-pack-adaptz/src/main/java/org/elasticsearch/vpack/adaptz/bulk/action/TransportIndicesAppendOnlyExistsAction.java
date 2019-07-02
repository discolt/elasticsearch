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

package org.elasticsearch.vpack.adaptz.bulk.action;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.*;

import static org.elasticsearch.vpack.adaptz.bulk.Constants.APPEND_WRITE_PREFIX;

public class TransportIndicesAppendOnlyExistsAction extends TransportMasterNodeReadAction<IndicesAppendOnlyExistsRequest, IndicesAppendOnlyExistsResponse> {

    @Inject
    public TransportIndicesAppendOnlyExistsAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                                  ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, IndicesAppendOnlyExistsAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, IndicesAppendOnlyExistsRequest::new);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected IndicesAppendOnlyExistsResponse newResponse() {
        return new IndicesAppendOnlyExistsResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(IndicesAppendOnlyExistsRequest request, ClusterState state) {
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(true, true, request.indicesOptions().expandWildcardsOpen(), request.indicesOptions().expandWildcardsClosed());
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, indexNameExpressionResolver.concreteIndexNames(state, indicesOptions, request.indices()));
    }

    @Override
    protected void masterOperation(final IndicesAppendOnlyExistsRequest request, final ClusterState state, final ActionListener<IndicesAppendOnlyExistsResponse> listener) {

        Set<String> indexNames = new HashSet();
        Iterator<ObjectCursor<String>> it = state.metaData().indices().keys().iterator();
        while ((it.hasNext())) {
            indexNames.add(it.next().value);
        }

        Map<String, Object> existMap = new HashMap();
        for (String index : request.indices()) {
            existMap.put(index, indexNames.contains(APPEND_WRITE_PREFIX + index));
        }
        listener.onResponse(new IndicesAppendOnlyExistsResponse(existMap));
    }

}
