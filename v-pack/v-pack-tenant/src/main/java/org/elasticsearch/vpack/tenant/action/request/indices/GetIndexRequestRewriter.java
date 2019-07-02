package org.elasticsearch.vpack.tenant.action.request.indices;

import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

public class GetIndexRequestRewriter implements ActionRequestRewriter<GetIndexRequest> {

    @Override
    public GetIndexRequest rewrite(GetIndexRequest request, String tenant) {
        if (request.indices().length == 0) {
            request.indices(tenant + "*");
        } else {
            request.indices(IndexTenantRewriter.rewrite(request.indices(), tenant));
        }
        return request;
    }
}
