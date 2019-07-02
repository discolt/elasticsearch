/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.vpack.xdcr.action.index;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.vpack.xdcr.utils.Actions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.vpack.XDCRPlugin.XDCR_FOLLOWING_INDEX_SETTING;

/**
 * 创建备集群索引
 */
public class PutIndexAction extends Action<PutIndexAction.Request, PutIndexAction.Response, PutIndexAction.Builder> {

    public static final String IMD_CUSTOM_XDCR = "xdcr";
    public static final String IMD_CUSTOM_XDCR_KEY_LEADER_INDEX_UUID = "leader_index_uuid";

    public static final PutIndexAction INSTANCE = new PutIndexAction();
    public static final String NAME = "indices:admin/xdcr/create";

    private PutIndexAction() {
        super(NAME);
    }

    @Override
    public Builder newRequestBuilder(ElasticsearchClient client) {
        return new Builder(client, INSTANCE, new Request());
    }

    @Override
    public Response newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public Writeable.Reader<Response> getResponseReader() {
        return Response::new;
    }

    /**
     * Request
     */
    public static class Request extends AcknowledgedRequest<Request> {

        private IndexMetaData indexMetaData;

        private Request() {
        }

        public Request(IndexMetaData indexMetaData) {
            this.indexMetaData = indexMetaData;
        }

        public String index() {
            return indexMetaData.getIndex().getName();
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            indexMetaData = IndexMetaData.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            indexMetaData.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return Actions.notNull("index_meta_data", indexMetaData);
        }
    }

    /**
     * RequestBuilder
     */
    public static class Builder extends MasterNodeOperationRequestBuilder<Request, Response, Builder> {
        protected Builder(ElasticsearchClient client, Action<Request, Response, Builder> action, Request request) {
            super(client, action, request);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final boolean followIndexCreated;
        private final boolean followIndexShardsAcked;
        private final boolean indexFollowingStarted;

        public Response(boolean followIndexCreated, boolean followIndexShardsAcked, boolean indexFollowingStarted) {
            this.followIndexCreated = followIndexCreated;
            this.followIndexShardsAcked = followIndexShardsAcked;
            this.indexFollowingStarted = indexFollowingStarted;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            followIndexCreated = in.readBoolean();
            followIndexShardsAcked = in.readBoolean();
            indexFollowingStarted = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(followIndexCreated);
            out.writeBoolean(followIndexShardsAcked);
            out.writeBoolean(indexFollowingStarted);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("follow_index_created", followIndexCreated);
                builder.field("follow_index_shards_acked", followIndexShardsAcked);
                builder.field("index_following_started", indexFollowingStarted);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return followIndexCreated == response.followIndexCreated &&
                    followIndexShardsAcked == response.followIndexShardsAcked &&
                    indexFollowingStarted == response.indexFollowingStarted;
        }

        @Override
        public int hashCode() {
            return Objects.hash(followIndexCreated, followIndexShardsAcked, indexFollowingStarted);
        }
    }

    /**
     * Transport
     */
    public static class Transport extends TransportMasterNodeAction<Request, Response> {

        private final Client client;
        private final AllocationService allocationService;
        private final ActiveShardsObserver activeShardsObserver;

        @Inject
        public Transport(
                final Settings settings,
                final ThreadPool threadPool,
                final TransportService transportService,
                final ClusterService clusterService,
                final ActionFilters actionFilters,
                final IndexNameExpressionResolver indexNameExpressionResolver,
                final Client client,
                final AllocationService allocationService) {
            super(
                    settings,
                    PutIndexAction.NAME,
                    transportService,
                    clusterService,
                    threadPool,
                    actionFilters,
                    Request::new,
                    indexNameExpressionResolver);
            this.client = client;
            this.allocationService = allocationService;
            this.activeShardsObserver = new ActiveShardsObserver(clusterService, threadPool);
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected Response newResponse() {
            throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
        }

        @Override
        protected Response read(StreamInput in) throws IOException {
            return new Response(in);
        }

        @Override
        protected void masterOperation(
                final Request request,
                final ClusterState state,
                final ActionListener<Response> listener) throws Exception {
            createFollowerIndex(request, listener);
        }

        private void createFollowerIndex(
                final Request request,
                final ActionListener<Response> listener) {
            IndexMetaData leaderIndexMetaData = request.indexMetaData;
            if (leaderIndexMetaData == null) {
                listener.onFailure(new IllegalArgumentException("leader index [" + request.index() + "] does not exist"));
                return;
            }

            ActionListener<Boolean> handler = ActionListener.wrap(
                    result -> listener.onResponse(new Response(true, false, false)),
                    listener::onFailure);
            clusterService.submitStateUpdateTask("create_following_index", new AckedClusterStateUpdateTask<Boolean>(request, handler) {

                @Override
                protected Boolean newResponse(final boolean acknowledged) {
                    return acknowledged;
                }

                @Override
                public ClusterState execute(final ClusterState currentState) throws Exception {
                    String followIndex = request.index();
                    IndexMetaData currentIndex = currentState.metaData().index(followIndex);
                    if (currentIndex != null) {
                        throw new ResourceAlreadyExistsException(currentIndex.getIndex());
                    }

                    MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                    IndexMetaData.Builder imdBuilder = IndexMetaData.builder(followIndex);

                    // Adding the leader index uuid for each shard as custom metadata:
                    Map<String, String> metadata = new HashMap<>();
                    metadata.put(IMD_CUSTOM_XDCR_KEY_LEADER_INDEX_UUID, leaderIndexMetaData.getIndexUUID());
                    imdBuilder.putCustom(IMD_CUSTOM_XDCR, metadata);

                    // Copy all settings, but overwrite a few settings.
                    Settings.Builder settingsBuilder = Settings.builder();
                    settingsBuilder.put(leaderIndexMetaData.getSettings());
                    settingsBuilder.put(XDCR_FOLLOWING_INDEX_SETTING.getKey(), true);
                    // Overwriting UUID here, because otherwise we can't follow indices in the same cluster
                    settingsBuilder.put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID());
                    settingsBuilder.put(IndexMetaData.SETTING_INDEX_PROVIDED_NAME, followIndex);
                    settingsBuilder.put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true);
                    imdBuilder.settings(settingsBuilder);

                    // Copy mappings from leader IMD to follow IMD
                    for (ObjectObjectCursor<String, MappingMetaData> cursor : leaderIndexMetaData.getMappings()) {
                        imdBuilder.putMapping(cursor.value);
                    }
                    imdBuilder.setRoutingNumShards(leaderIndexMetaData.getRoutingNumShards());
                    IndexMetaData followIMD = imdBuilder.build();
                    mdBuilder.put(followIMD, false);

                    ClusterState.Builder builder = ClusterState.builder(currentState);
                    builder.metaData(mdBuilder.build());
                    ClusterState updatedState = builder.build();

                    RoutingTable.Builder routingTableBuilder = RoutingTable.builder(updatedState.routingTable())
                            .addAsNew(updatedState.metaData().index(request.index()));
                    updatedState = allocationService.reroute(
                            ClusterState.builder(updatedState).routingTable(routingTableBuilder.build()).build(),
                            "follow index [" + request.index() + "] created");

                    logger.info("[{}] creating index, cause [xdcr_create_and_follow], shards [{}]/[{}]",
                            followIndex, followIMD.getNumberOfShards(), followIMD.getNumberOfReplicas());

                    return updatedState;
                }
            });
        }

        @Override
        protected ClusterBlockException checkBlock(final Request request, final ClusterState state) {
            return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.index());
        }
    }

}
