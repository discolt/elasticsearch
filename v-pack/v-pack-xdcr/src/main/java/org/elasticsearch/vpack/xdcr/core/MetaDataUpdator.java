package org.elasticsearch.vpack.xdcr.core;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.vpack.xdcr.core.follower.FollowerHandlerRegister;
import org.elasticsearch.transport.TransportFuture;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

/**
 * 发起端
 */
public class MetaDataUpdator {

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final String repository;
    private final String index;

    public MetaDataUpdator(ClusterService clusterService, TransportService transportService, String repository, String index) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.repository = repository;
        this.index = index;
    }

    public void createOrUpdateOnFollower() {
        DiscoveryNode targetNode = transportService.getRemoteClusterService().getConnection(repository).getNode();
        IndexMetaData indexMetaData = clusterService.state().metaData().index(index);
        TransportRequest request = new FollowerHandlerRegister.UpdateIndexMetaDataRequest(indexMetaData);
        TransportFuture<FollowerHandlerRegister.UpdateIndexMetaDataResponse> future = transportService.submitRequest(
                targetNode,
                FollowerHandlerRegister.Actions.PROXY_CREATE_OR_UPDATE_INDEX,
                request,
                TransportRequestOptions.EMPTY,
                FollowerHandlerRegister.UpdateIndexMetaDataResponse.HANDLER
        );
        future.txGet();
    }

}
