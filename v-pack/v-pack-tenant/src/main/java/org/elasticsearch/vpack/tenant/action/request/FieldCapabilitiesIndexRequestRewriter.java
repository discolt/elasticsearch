package org.elasticsearch.vpack.tenant.action.request;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

public class FieldCapabilitiesIndexRequestRewriter implements ActionRequestRewriter<FieldCapabilitiesRequest> {

    @Override
    public FieldCapabilitiesRequest rewrite(FieldCapabilitiesRequest request, String tenant) {
        request.indices(IndexTenantRewriter.rewrite(request.indices(), tenant, true));
        return request;
    }
}
