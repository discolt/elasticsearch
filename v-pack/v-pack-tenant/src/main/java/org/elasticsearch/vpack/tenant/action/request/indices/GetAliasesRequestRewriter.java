package org.elasticsearch.vpack.tenant.action.request.indices;

import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

public class GetAliasesRequestRewriter implements ActionRequestRewriter<GetAliasesRequest> {

    @Override
    public GetAliasesRequest rewrite(GetAliasesRequest request, String tenant) {
        if (request.indices().length == 0) {
            request.indices(tenant + ".*");
        } else {
            request.indices(IndexTenantRewriter.rewrite(request.indices(), tenant));
        }
        request.aliases(IndexTenantRewriter.rewrite(request.aliases(), tenant));
        return request;
    }
}
