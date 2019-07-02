package org.elasticsearch.vpack.tenant.rest.request;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.rest.RestRequestRewriter;

import java.io.IOException;
import java.util.Optional;

public class RestPainlessExecuteRequestRewriter implements RestRequestRewriter {

    @Override
    public Optional<ContentRewriter> contentRewriter() {
        return Optional.of((content, tenant) -> rewrite(content, tenant));
    }

    private static String rewrite(String content, String tenant) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode sourceNode = mapper.readTree(content);
        IndexTenantRewriter.rewriteJson(sourceNode, "context_setup.index", tenant, true);
        return sourceNode.toString();
    }

}
