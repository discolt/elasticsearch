package org.elasticsearch.vpack.tenant.action.request.indices;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

public class IndicesAliasesRequestRewriter implements ActionRequestRewriter<IndicesAliasesRequest> {

    @Override
    public IndicesAliasesRequest rewrite(IndicesAliasesRequest request, String tenant) {
        request.getAliasActions().forEach(e -> {
            e.replaceAliases(IndexTenantRewriter.rewrite(e.aliases(), tenant));
            e.indices(IndexTenantRewriter.rewrite(e.indices(), tenant));
        });
        return request;
    }
}
