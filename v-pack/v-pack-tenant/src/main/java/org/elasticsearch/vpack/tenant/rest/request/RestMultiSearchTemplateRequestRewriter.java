package org.elasticsearch.vpack.tenant.rest.request;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.rest.RestRequestRewriter;

import java.io.IOException;
import java.util.Optional;
import java.util.Scanner;

public class RestMultiSearchTemplateRequestRewriter implements RestRequestRewriter {

    @Override
    public Optional<ContentRewriter> contentRewriter() {
        return Optional.of((content, tenant) -> rewrite(content, tenant));
    }

    private static String rewrite(String content, String tenant) throws IOException {
        StringBuffer buffer = new StringBuffer();
        Scanner scanner = new Scanner(content);
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            line = replaceFieldWithTenant(line, "id", tenant);
            buffer.append(line).append("\n");
        }
        scanner.close();
        buffer.append("\n");
        return buffer.toString();
    }

    private static String replaceFieldWithTenant(String line, String field, String tenant) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(line);
        if (root.has(field)) {
            IndexTenantRewriter.rewriteJson(root, field, tenant);
        }
        return root.toString();
    }

}
