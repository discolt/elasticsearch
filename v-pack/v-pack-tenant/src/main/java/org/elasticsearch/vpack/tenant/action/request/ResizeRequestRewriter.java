package org.elasticsearch.vpack.tenant.action.request;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;
import org.elasticsearch.vpack.tenant.action.request.indices.CreateIndexRequestRewriter;

public class ResizeRequestRewriter implements ActionRequestRewriter<ResizeRequest> {

    @Override
    public ResizeRequest rewrite(ResizeRequest request, String tenant) {
        request.setSourceIndex(IndexTenantRewriter.rewrite(request.getSourceIndex(), tenant));
        CreateIndexRequest targetIndexRequest = request.getTargetIndexRequest();
        CreateIndexRequestRewriter.rebuild(targetIndexRequest, tenant);
        return request;
    }
}
