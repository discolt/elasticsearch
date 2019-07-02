package org.elasticsearch.vpack.tenant.action.request;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

public class BulkRequestRewriter implements ActionRequestRewriter<BulkRequest> {

    @Override
    public BulkRequest rewrite(BulkRequest request, String tenant) {
        request.requests().forEach(e -> {
            e.index(IndexTenantRewriter.rewrite(e.index(), tenant));
            if (e instanceof IndexRequest) {
                ((IndexRequest) e).setPipeline(IndexTenantRewriter.rewrite(((IndexRequest) e).getPipeline(), tenant));
            }
        });
        return request;
    }

}
