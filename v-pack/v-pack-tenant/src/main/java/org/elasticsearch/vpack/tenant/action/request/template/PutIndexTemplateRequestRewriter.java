package org.elasticsearch.vpack.tenant.action.request.template;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

public class PutIndexTemplateRequestRewriter implements ActionRequestRewriter<PutIndexTemplateRequest> {

    @Override
    public PutIndexTemplateRequest rewrite(PutIndexTemplateRequest request, String tenant) {
        PutIndexTemplateRequest replaced = new PutIndexTemplateRequest(request.name());
        replaced.name(IndexTenantRewriter.rewrite(request.name(), tenant));
        replaced.patterns(IndexTenantRewriter.rewrite(request.patterns(), tenant));
        request.aliases().forEach(e -> {
            Alias newAlias = new Alias(IndexTenantRewriter.rewrite(e.name(), tenant));
            newAlias.writeIndex(e.writeIndex());
            newAlias.searchRouting(e.searchRouting());
            newAlias.indexRouting(e.indexRouting());
            newAlias.filter(e.filter());
            replaced.alias(newAlias);
        });
        request.mappings().forEach((k, v) -> {
            replaced.mapping(k, v, XContentType.JSON);
        });
        replaced.order(replaced.order());
        replaced.cause(request.cause());
        replaced.version(request.version());
        replaced.settings(request.settings());
        replaced.create(request.create());
        replaced.settings(request.settings());
        return replaced;
    }

}
