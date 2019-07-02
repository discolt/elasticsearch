package org.elasticsearch.vpack.tenant.action.request;

import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

public class ClearScrollRequestRewriter implements ActionRequestRewriter<ClearScrollRequest> {

    @Override
    public ClearScrollRequest rewrite(ClearScrollRequest request, String tenant) {
        if (request.getScrollIds().contains("_all")) {
            throw new UnsupportedOperationException("delete scroll id parameter '_all' is unsupported");
        }
        return request;
    }
}
