package org.elasticsearch.vpack.tenant.action.request.indices;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

public class GetMappingsRequestRewriter implements ActionRequestRewriter<GetMappingsRequest> {
    @Override
    public GetMappingsRequest rewrite(GetMappingsRequest request, String tenant) {
        if (request.indices().length == 0) {
            request.indices(tenant + ".*");
        } else {
            request.indices(IndexTenantRewriter.rewrite(request.indices(), tenant));
        }
        return request;
    }
}
