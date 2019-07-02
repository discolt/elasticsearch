package org.elasticsearch.vpack.tenant.action.request.script;

import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

public class DeleteStoredScriptRequestRewriter implements ActionRequestRewriter<DeleteStoredScriptRequest> {


    @Override
    public DeleteStoredScriptRequest rewrite(DeleteStoredScriptRequest request, String tenant) {
        request.id(IndexTenantRewriter.rewrite(request.id(), tenant));
        return request;
    }
}
