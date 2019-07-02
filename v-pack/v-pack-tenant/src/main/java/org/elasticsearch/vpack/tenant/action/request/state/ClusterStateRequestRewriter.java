package org.elasticsearch.vpack.tenant.action.request.state;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

public class ClusterStateRequestRewriter implements ActionRequestRewriter<ClusterStateRequest> {

    @Override
    public ClusterStateRequest rewrite(ClusterStateRequest request, String tenant) {
//        if (request.indices().length == 0) {
//            request.indices(tenant + ".*");
//        } else {
//            request.indices(IndexTenantRewriter.rewrite(request.indices(), tenant));
//        }
        return request;
    }

}
