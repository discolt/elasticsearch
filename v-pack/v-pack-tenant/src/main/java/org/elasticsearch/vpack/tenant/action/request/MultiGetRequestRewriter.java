package org.elasticsearch.vpack.tenant.action.request;

import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

public class MultiGetRequestRewriter implements ActionRequestRewriter<MultiGetRequest> {

    @Override
    public MultiGetRequest rewrite(MultiGetRequest request, String tenant) {
        request.getItems().forEach(
                e -> e.index(IndexTenantRewriter.rewrite(e.index(), tenant))
        );
        return request;
    }
}
