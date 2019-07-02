package org.elasticsearch.vpack.tenant.action.request.pipeline;

import org.elasticsearch.action.ingest.GetPipelineRequest;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

public class GetPipelineRequestRewriter implements ActionRequestRewriter<GetPipelineRequest> {

    @Override
    public GetPipelineRequest rewrite(GetPipelineRequest request, String tenant) {
        if (request.getIds().length == 0) {
            return new GetPipelineRequest(new String[]{tenant + ".*"});
        } else {
            return new GetPipelineRequest(IndexTenantRewriter.rewrite(request.getIds(), tenant));
        }
    }
}
