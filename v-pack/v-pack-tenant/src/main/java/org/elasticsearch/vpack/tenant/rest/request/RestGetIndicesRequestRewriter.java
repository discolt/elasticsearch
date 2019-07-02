package org.elasticsearch.vpack.tenant.rest.request;

import org.elasticsearch.vpack.tenant.PathTenantRewriter;
import org.elasticsearch.vpack.tenant.rest.RestRequestRewriter;

import java.util.Optional;

public class RestGetIndicesRequestRewriter implements RestRequestRewriter {

    @Override
    public Optional<ParamsRewriter> paramsRewriter() {
        return Optional.of(
                (params, tenant) -> {
                    params.put("index", PathTenantRewriter.rewrite(params.get("index"), tenant));
                }
        );
    }
}
