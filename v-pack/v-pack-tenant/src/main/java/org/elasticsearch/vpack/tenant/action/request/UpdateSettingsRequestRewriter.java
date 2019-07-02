package org.elasticsearch.vpack.tenant.action.request;

import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

public class UpdateSettingsRequestRewriter implements ActionRequestRewriter<UpdateSettingsRequest> {

    @Override
    public UpdateSettingsRequest rewrite(UpdateSettingsRequest request, String tenant) {
        request.indices(IndexTenantRewriter.rewrite(request.indices(), tenant, true));
        String pipeline = request.settings().get("index.default_pipeline");
        if (pipeline != null) {
            Settings.Builder builder = Settings.builder();
            builder.put(request.settings());
            builder.put("index.default_pipeline", IndexTenantRewriter.rewrite(pipeline, tenant));
            request.settings(builder);
        }
        return request;
    }

}
