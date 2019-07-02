package org.elasticsearch.vpack.xdcr.core.follower;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.elasticsearch.vpack.xdcr.core.follower.bulk.BulkShardOperationsRequest;
import org.elasticsearch.vpack.xdcr.core.follower.bulk.BulkShardOperationsResponse;
import org.elasticsearch.vpack.xdcr.core.follower.bulk.TransportBulkShardOperationsAction;

import java.io.IOException;
import java.util.List;

/**
 * 接收端
 */
public class FollowerHandlerRegister extends AbstractComponent {

    public static class Actions {
        public static final String TRANSLOG_OPS = "indices:internal/xdcr/write_translog";
        public static final String CREATE_OR_UPDATE_INDEX = "indices:internal/xdcr/sync_meta";
        public static final String PROXY_TRANSLOG_OPS = TRANSLOG_OPS + "/proxy";
        public static final String PROXY_CREATE_OR_UPDATE_INDEX = CREATE_OR_UPDATE_INDEX + "/proxy";
    }

    private final Client client;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final IndexScopedSettings indexScopedSettings;
    private final TransportBulkShardOperationsAction bulkShardOperationsAction;

    @Inject
    public FollowerHandlerRegister(Settings settings, TransportService transportService,
                                   ClusterService clusterService, Client client,
                                   IndexScopedSettings indexScopedSettings,
                                   TransportBulkShardOperationsAction bulkShardOperationsAction
    ) {
        super(settings);
        this.client = client;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indexScopedSettings = indexScopedSettings;
        this.bulkShardOperationsAction = bulkShardOperationsAction;

        /**
         * 同步表结构
         */
        transportService.registerRequestHandler(
                Actions.PROXY_CREATE_OR_UPDATE_INDEX,
                UpdateIndexMetaDataRequest::new,
                ThreadPool.Names.GENERIC,
                new ProxySyncIndexMetaHandler());
        transportService.registerRequestHandler(
                Actions.CREATE_OR_UPDATE_INDEX,
                UpdateIndexMetaDataRequest::new,
                ThreadPool.Names.GENERIC,
                new SyncIndexMetaHandler());

        /**
         * 同步translog
         */
        transportService.registerRequestHandler(
                Actions.PROXY_TRANSLOG_OPS,
                TranslogOpsRequest::new,
                ThreadPool.Names.GENERIC,
                new ProxyTranslogOpsRequestHandler());
    }

    /**
     * ===============================================================
     * -----------------------  同步MetaData部分 ------------------------
     * ===============================================================
     */

    public static class UpdateIndexMetaDataRequest extends TransportRequest {

        private IndexMetaData indexMetaData;

        UpdateIndexMetaDataRequest() {
        }

        public UpdateIndexMetaDataRequest(IndexMetaData indexMetaData) {
            this.indexMetaData = indexMetaData;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            indexMetaData = IndexMetaData.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            indexMetaData.writeTo(out);
        }
    }

    public static class UpdateIndexMetaDataResponse extends TransportResponse {

        private boolean updated;

        public boolean isUpdated() {
            return updated;
        }

        public UpdateIndexMetaDataResponse() {

        }

        public UpdateIndexMetaDataResponse(boolean updated) {
            this.updated = updated;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            updated = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(updated);
        }

        public static TransportResponseHandler<UpdateIndexMetaDataResponse> HANDLER =
                new FutureTransportResponseHandler<UpdateIndexMetaDataResponse>() {
                    @Override
                    public UpdateIndexMetaDataResponse newInstance() {
                        return new UpdateIndexMetaDataResponse();
                    }
                };
    }

    /**
     * 代理节点处理
     * 转发到真实节点
     */
    class ProxySyncIndexMetaHandler implements TransportRequestHandler<UpdateIndexMetaDataRequest> {

        @Override
        public void messageReceived(final UpdateIndexMetaDataRequest request, final TransportChannel channel) throws IOException {
            DiscoveryNode node = getMaster(clusterService);
            TransportFuture<UpdateIndexMetaDataResponse> future = transportService.submitRequest(
                    node,
                    Actions.CREATE_OR_UPDATE_INDEX,
                    request,
                    TransportRequestOptions.EMPTY,
                    UpdateIndexMetaDataResponse.HANDLER
            );
            channel.sendResponse(future.txGet());
        }
    }

    /**
     * 真实节点处理
     */
    class SyncIndexMetaHandler implements TransportRequestHandler<UpdateIndexMetaDataRequest> {

        @Override
        public void messageReceived(final UpdateIndexMetaDataRequest request, final TransportChannel channel) throws IOException {
            UpdateIndexMetaDataOnFollower indexMetaDataUpdator = new UpdateIndexMetaDataOnFollower(clusterService, client, indexScopedSettings);
            indexMetaDataUpdator.createOrUpdate(request.indexMetaData);
            channel.sendResponse(new UpdateIndexMetaDataResponse());
        }
    }

    /**
     * ===============================================================
     * -----------------------  恢复translog部分 ----------------------
     * ===============================================================
     */

    public static class TranslogOpsRequest extends TransportRequest {

        private String index;
        private int shard;
        private List<Translog.Operation> operations;

        TranslogOpsRequest() {
        }

        public TranslogOpsRequest(String index, int shard, List<Translog.Operation> operations) {
            this.index = index;
            this.shard = shard;
            this.operations = operations;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            index = in.readString();
            shard = in.readInt();
            operations = Translog.readOperations(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(index);
            out.writeInt(shard);
            Translog.writeOperations(out, operations);
        }
    }

    public static class TranslogOpsResponse extends TransportResponse {

        public TranslogOpsResponse() {
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        public static TransportResponseHandler<TranslogOpsResponse> HANDLER =
                new FutureTransportResponseHandler<TranslogOpsResponse>() {
                    @Override
                    public TranslogOpsResponse newInstance() {
                        return new TranslogOpsResponse();
                    }
                };
    }

    /**
     * 代理节点处理
     * 转发到真实节点
     */
    class ProxyTranslogOpsRequestHandler implements TransportRequestHandler<TranslogOpsRequest> {

        @Override
        public void messageReceived(final TranslogOpsRequest request, final TransportChannel channel) {
            ShardId shardId = shardId(clusterService, request.index, request.shard);
            final BulkShardOperationsRequest coordinateRequest = new BulkShardOperationsRequest(shardId, request.operations);
            bulkShardOperationsAction.execute(coordinateRequest, new ActionListener<BulkShardOperationsResponse>() {
                @Override
                public void onResponse(BulkShardOperationsResponse bulkShardOperationsResponse) {
                    try {
                        channel.sendResponse(new TranslogOpsResponse());
                    } catch (IOException e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception err) {
                        logger.error("[xdcr] bulk error", err);
                        throw new RuntimeException(err);
                    }
                }
            });

        }
    }

    /**
     * 获取主节点
     *
     * @param clusterService
     * @return
     */
    public static DiscoveryNode getMaster(ClusterService clusterService) {
        return clusterService.state().nodes().getMasterNode();
    }

    public static ShardId shardId(ClusterService clusterService, String index, int shard) {
        ClusterState clusterState = clusterService.state();
        return clusterState.getRoutingTable().index(index).shard(shard).shardId();
    }

}
