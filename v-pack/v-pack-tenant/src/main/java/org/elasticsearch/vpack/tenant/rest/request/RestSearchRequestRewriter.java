package org.elasticsearch.vpack.tenant.rest.request;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.rest.RestRequestRewriter;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class RestSearchRequestRewriter implements RestRequestRewriter {


    @Override
    public Optional<ContentRewriter> contentRewriter() {
        return Optional.of((content, tenant) -> rewrite(content, tenant));
    }

    private static String rewrite(String content, String tenant) throws IOException {
        String source = content;
        if (!source.contains("indices_boost")) {
            return content;
        }
        ObjectMapper mapper = new ObjectMapper();
        JsonNode sourceNode = mapper.readTree(source);
        JsonNode boostNode = sourceNode.get("indices_boost");
        if (boostNode != null) {
            if (boostNode instanceof ArrayNode) {
                for (int i = 0; i < boostNode.size(); i++) {
                    JsonNode indexBoosts = boostNode.get(i);
                    Map.Entry<String, JsonNode> entry = indexBoosts.fields().next();
                    ((ObjectNode) indexBoosts).remove(entry.getKey());
                    ((ObjectNode) indexBoosts).set(IndexTenantRewriter.rewrite(entry.getKey(), tenant), entry.getValue());
                }
            }
            if (boostNode instanceof ObjectNode) {
                for (int i = 0; i < boostNode.size(); i++) {
                    Map.Entry<String, JsonNode> entry = boostNode.fields().next();
                    ((ObjectNode) boostNode).remove(entry.getKey());
                    ((ObjectNode) boostNode).set(IndexTenantRewriter.rewrite(entry.getKey(), tenant), entry.getValue());
                }
            }
        }
        return sourceNode.toString();
    }


}