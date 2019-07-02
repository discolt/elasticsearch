package org.elasticsearch.vpack.tenant.action.request;

import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

public class ReindexRequestRewriter implements ActionRequestRewriter<ReindexRequest> {
    @Override
    public ReindexRequest rewrite(ReindexRequest request, String tenant) {
        RemoteInfo remoteInfo = request.getRemoteInfo();
        if (remoteInfo != null) {
            throw new UnsupportedOperationException("reindex disallow remote cluster");
        }
        request.setSourceIndices(IndexTenantRewriter.rewrite(request.getSearchRequest().indices(), tenant));
        request.setDestIndex(IndexTenantRewriter.rewrite(request.getDestination().index(), tenant));
        request.setDestPipeline(IndexTenantRewriter.rewrite(request.getDestination().getPipeline(), tenant));
        return request;
    }

}
