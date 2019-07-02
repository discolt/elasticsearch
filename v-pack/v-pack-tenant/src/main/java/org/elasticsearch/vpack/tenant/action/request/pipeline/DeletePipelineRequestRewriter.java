package org.elasticsearch.vpack.tenant.action.request.pipeline;

import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

public class DeletePipelineRequestRewriter implements ActionRequestRewriter<DeletePipelineRequest> {

    @Override
    public DeletePipelineRequest rewrite(DeletePipelineRequest request, String tenant) {
        request.setId(IndexTenantRewriter.rewrite(request.getId(), tenant));
        return request;
    }
}
