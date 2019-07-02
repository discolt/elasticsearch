package org.elasticsearch.vpack.tenant.action.request;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

public class GetSettingsRequestRewriter implements ActionRequestRewriter<GetSettingsRequest> {

    @Override
    public GetSettingsRequest rewrite(GetSettingsRequest request, String tenant) {
        if (request.indices().length == 0) {
            request.indices(tenant + ".*");
        } else {
            request.indices(IndexTenantRewriter.rewrite(request.indices(), tenant));
        }
        return request;
    }
}
