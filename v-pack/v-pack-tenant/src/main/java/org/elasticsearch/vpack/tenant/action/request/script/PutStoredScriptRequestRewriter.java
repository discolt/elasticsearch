package org.elasticsearch.vpack.tenant.action.request.script;

import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

public class PutStoredScriptRequestRewriter implements ActionRequestRewriter<PutStoredScriptRequest> {

    @Override
    public PutStoredScriptRequest rewrite(PutStoredScriptRequest request, String tenant) {
        request.id(IndexTenantRewriter.rewrite(request.id(), tenant));
        return request;
    }
}
