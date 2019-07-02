package org.elasticsearch.vpack.tenant.rest.request;

import org.elasticsearch.vpack.tenant.PathTenantRewriter;
import org.elasticsearch.vpack.tenant.rest.RestRequestRewriter;

import java.util.Optional;

public class RestGetAliasRequestRewriter implements RestRequestRewriter {

    @Override
    public Optional<ParamsRewriter> paramsRewriter() {
        return Optional.of(
                (params, tenant) -> {
                    String alias = params.get("name");
                    if (alias != null && !alias.equals("*") && !alias.equals("_all")) {
                        params.put("name", PathTenantRewriter.rewrite(alias, tenant));
                    }
                }
        );
    }
}
