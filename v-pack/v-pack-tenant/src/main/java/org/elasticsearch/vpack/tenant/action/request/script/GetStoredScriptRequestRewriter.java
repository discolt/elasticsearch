package org.elasticsearch.vpack.tenant.action.request.script;

import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

public class GetStoredScriptRequestRewriter implements ActionRequestRewriter<GetStoredScriptRequest> {

    @Override
    public GetStoredScriptRequest rewrite(GetStoredScriptRequest request, String tenant) {
        request.id(IndexTenantRewriter.rewrite(request.id(), tenant));
        return request;
    }
}
