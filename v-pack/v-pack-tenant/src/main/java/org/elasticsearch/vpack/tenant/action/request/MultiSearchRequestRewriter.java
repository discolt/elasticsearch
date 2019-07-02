package org.elasticsearch.vpack.tenant.action.request;

import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

public class MultiSearchRequestRewriter implements ActionRequestRewriter<MultiSearchRequest> {

    @Override
    public MultiSearchRequest rewrite(MultiSearchRequest request, String tenant) {
        request.requests().forEach(e -> e.indices(IndexTenantRewriter.rewrite(e.indices(), tenant)));
        return request;
    }

}
