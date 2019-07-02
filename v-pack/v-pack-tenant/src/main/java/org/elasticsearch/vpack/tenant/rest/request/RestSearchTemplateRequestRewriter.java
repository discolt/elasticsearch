package org.elasticsearch.vpack.tenant.rest.request;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.common.Strings;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.PathTenantRewriter;
import org.elasticsearch.vpack.tenant.rest.RestRequestRewriter;

import java.io.IOException;
import java.util.Optional;

public class RestSearchTemplateRequestRewriter implements RestRequestRewriter {

    @Override
    public Optional<ParamsRewriter> paramsRewriter() {
        return Optional.of(
                (params, tenant) -> {
                    String id = params.get("id");
                    if (!Strings.isNullOrEmpty(id)) {
                        params.put("id", PathTenantRewriter.rewrite(id, tenant));
                    }
                }
        );
    }

    @Override
    public Optional<ContentRewriter> contentRewriter() {
        return Optional.of((content, tenant) -> rewrite(content, tenant));
    }

    private static String rewrite(String content, String tenant) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode sourceNode = mapper.readTree(content);
        IndexTenantRewriter.rewriteJson(sourceNode, "id", tenant);
        return sourceNode.toString();
    }

}
