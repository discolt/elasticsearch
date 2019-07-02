package org.elasticsearch.vpack.tenant.action.request;

import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

public class RolloverRequestRewriter implements ActionRequestRewriter<RolloverRequest> {

    @Override
    public RolloverRequest rewrite(RolloverRequest request, String tenant) {
        request.setNewIndexName(IndexTenantRewriter.rewrite(request.getNewIndexName(), tenant));
        request.setAlias(IndexTenantRewriter.rewrite(request.getAlias(), tenant));
        return request;
    }
}
