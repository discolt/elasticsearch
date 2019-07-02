package org.elasticsearch.vpack.tenant.action.request;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

public class SearchRequestRewriter implements ActionRequestRewriter<SearchRequest> {

    @Override
    public SearchRequest rewrite(SearchRequest request, String tenant) {
        if (request.indices().length == 0) {
            request.indices(tenant + ".*");
        } else {
            request.indices(IndexTenantRewriter.rewrite(request.indices(), tenant));
        }
        return request;
    }

}
