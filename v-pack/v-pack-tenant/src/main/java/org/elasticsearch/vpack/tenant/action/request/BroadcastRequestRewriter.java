package org.elasticsearch.vpack.tenant.action.request;

import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

public class BroadcastRequestRewriter implements ActionRequestRewriter<BroadcastRequest> {
    @Override
    public BroadcastRequest rewrite(BroadcastRequest request, String tenant) {
        if (request.indices().length == 0) {
            request.indices(tenant + ".*");
        } else {
            request.indices(IndexTenantRewriter.rewrite(request.indices(), tenant));
        }
        return request;
    }
}
