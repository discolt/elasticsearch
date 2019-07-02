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

package org.elasticsearch.vpack.xdcr.action.feed.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.vpack.xdcr.cluster.PushingService;
import org.elasticsearch.vpack.xdcr.metadata.Feed;
import org.elasticsearch.vpack.xdcr.metadata.FeedInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Transport action for create feed operation
 */
public class TransportCreateFeedAction extends TransportMasterNodeAction<CreateFeedRequest, CreateFeedResponse> {
    private final PushingService pushingService;

    @Inject
    public TransportCreateFeedAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                     ThreadPool threadPool, PushingService pushingService, ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, CreateFeedAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, CreateFeedRequest::new);
        this.pushingService = pushingService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SNAPSHOT;
    }

    @Override
    protected CreateFeedResponse newResponse() {
        return new CreateFeedResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(CreateFeedRequest request, ClusterState state) {
        ClusterBlockException clusterBlockException = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
        return clusterBlockException;
    }

    @Override
    protected void masterOperation(final CreateFeedRequest request, ClusterState state, final ActionListener<CreateFeedResponse> listener) {
        PushingService.FeedRequest feedRequest =
                new PushingService.FeedRequest(request.repository(), request.indices(), "create_feed [" + request.indices() + "]")
                        .settings(request.settings())
                        .masterNodeTimeout(request.masterNodeTimeout());
        pushingService.createPushingState(feedRequest, new PushingService.CreateFeedListener() {
            @Override
            public void onResponse() {
                if (request.waitForCompletion()) {
                    pushingService.addListener(new PushingService.SnapshotCompletionListener() {
                        @Override
                        public void onFeedPushingCompletion(Feed snapshot, FeedInfo feedInfo) {
                            if (snapshot.getRepository().equals(request.repository()) &&
                                    snapshot.getFeedId().getName().equals(request.indices())) {
                                listener.onResponse(new CreateFeedResponse(feedInfo));
                                pushingService.removeListener(this);
                            }
                        }

                        @Override
                        public void onFeedPushingFailure(Feed feed, Exception e) {
                            if (feed.getRepository().equals(request.repository()) &&
                                    feed.getFeedId().getName().equals(request.indices())) {
                                listener.onFailure(e);
                                pushingService.removeListener(this);
                            }
                        }
                    });
                } else {
                    listener.onResponse(new CreateFeedResponse());
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
