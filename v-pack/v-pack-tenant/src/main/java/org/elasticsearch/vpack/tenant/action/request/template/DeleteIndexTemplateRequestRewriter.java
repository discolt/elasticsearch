package org.elasticsearch.vpack.tenant.action.request.template;

import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

public class DeleteIndexTemplateRequestRewriter implements ActionRequestRewriter<DeleteIndexTemplateRequest> {
    @Override
    public DeleteIndexTemplateRequest rewrite(DeleteIndexTemplateRequest request, String tenant) {
        request.name(IndexTenantRewriter.rewrite(request.name(), tenant));
        return request;
    }
}
