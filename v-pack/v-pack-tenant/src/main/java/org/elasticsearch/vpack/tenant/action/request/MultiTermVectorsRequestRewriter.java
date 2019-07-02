package org.elasticsearch.vpack.tenant.action.request;

import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

public class MultiTermVectorsRequestRewriter implements ActionRequestRewriter<MultiTermVectorsRequest> {

    @Override
    public MultiTermVectorsRequest rewrite(MultiTermVectorsRequest request, String tenant) {
        request.getRequests().forEach(
                e -> {
                    e.index(IndexTenantRewriter.rewrite(e.index(), tenant));
                }
        );
        return request;
    }
}
