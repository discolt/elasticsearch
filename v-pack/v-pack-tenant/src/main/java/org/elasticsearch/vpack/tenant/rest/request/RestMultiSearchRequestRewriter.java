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
import java.util.Scanner;

public class RestMultiSearchRequestRewriter implements RestRequestRewriter {

    @Override
    public Optional<ContentRewriter> contentRewriter() {
        return Optional.of((content, tenant) -> rewrite(content, tenant));
    }

    private static String rewrite(String content, String tenant) throws IOException {
        StringBuffer buffer = new StringBuffer();
        Scanner scanner = new Scanner(content);
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            if (line.contains("indices_boost")) {
                buffer.append(replaceIndexBoost(line, tenant)).append("\n");
            } else {
                buffer.append(line).append("\n");
            }
        }
        scanner.close();
        return buffer.toString();
    }

    private static String replaceIndexBoost(String jsonLine, String appname) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode sourceNode = mapper.readTree(jsonLine);
        JsonNode boostNode = sourceNode.get("indices_boost");
        if (boostNode instanceof ArrayNode) {
            for (int i = 0; i < boostNode.size(); i++) {
                JsonNode indexBoosts = boostNode.get(i);
                Map.Entry<String, JsonNode> entry = indexBoosts.fields().next();
                ((ObjectNode) indexBoosts).remove(entry.getKey());
                ((ObjectNode) indexBoosts).set(IndexTenantRewriter.rewrite(entry.getKey(), appname), entry.getValue());
            }
        }
        if (boostNode instanceof ObjectNode) {
            for (int i = 0; i < boostNode.size(); i++) {
                Map.Entry<String, JsonNode> entry = boostNode.fields().next();
                ((ObjectNode) boostNode).remove(entry.getKey());
                ((ObjectNode) boostNode).set(IndexTenantRewriter.rewrite(entry.getKey(), appname), entry.getValue());
            }
        }
        return sourceNode.toString();
    }


}