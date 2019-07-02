package org.elasticsearch.vpack.tenant.action.request.template;

import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

public class GetIndexTemplatesRequestRewriter implements ActionRequestRewriter<GetIndexTemplatesRequest> {

    @Override
    public GetIndexTemplatesRequest rewrite(GetIndexTemplatesRequest request, String tenant) {
        if (request.names() == null || request.names().length == 0) {
            request.names(tenant + ".*");
        } else {
            request.names(IndexTenantRewriter.rewrite(request.names(), tenant));
        }
        return request;
    }

}
