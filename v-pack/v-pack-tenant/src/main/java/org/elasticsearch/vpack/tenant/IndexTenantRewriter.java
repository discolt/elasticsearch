package org.elasticsearch.vpack.tenant;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;
import java.util.stream.Collectors;

public class IndexTenantRewriter {

    public static List<String> rewrite(List<String> indices, String tenant) {
        if (indices == null || indices.size() == 0) {
            return indices;
        }
        List<String> rewrited = new ArrayList(indices.size());
        for (int i = 0; i < indices.size(); i++) {
            rewrited.add(rewrite(indices.get(i), tenant));
        }
        return rewrited;
    }

    public static Set<String> rewrite(Set<String> indices, String tenant) {
        if (indices == null || indices.size() == 0) {
            return indices;
        }
        Set<String> rewrited = new HashSet(indices.size());
        indices.forEach(e -> rewrite(e, tenant));
        return rewrited;
    }


    /**
     * 改写Indices
     *
     * @param indices indices
     * @param tenant tenant
     * @param rewriteEmpty true: 当indices为空时改写为tenant + ".*"
     * @return rewrited indices
     */
    public static String[] rewrite(String[] indices, String tenant, boolean rewriteEmpty) {
        if (indices == null || indices.length == 0) {
            if (rewriteEmpty)
                return new String[]{tenant + ".*"};
        }
        return rewrite(indices, tenant);
    }

    /**
     * 改写Indices
     * indices为空时不该写
     *
     * @param indices indices
     * @param tenant tenant
     * @return rewrited indices
     */
    public static String[] rewrite(String[] indices, String tenant) {
        if (indices == null || indices.length == 0) {
            return indices;
        }
        String[] rewrited = new String[indices.length];
        for (int i = 0; i < indices.length; i++) {
            rewrited[i] = rewrite(indices[i], tenant);
        }
        return rewrited;
    }

    public static String rewrite(String index, String tenant) {
        if (tenant == null || tenant.trim().length() == 0 || index == null || index.trim().length() == 0) {
            return index;
        }
        if (index.equals("*") || index.equals("_all")) {
            return tenant + ".*";
        }
        return rewriteByComma(index, tenant);
    }


    private static String rewriteByComma(String index, String tenant) {
        return Arrays.stream(index.split(","))
                .map(idx -> atom(idx, tenant))
                .collect(Collectors.joining(","));
    }

    // 原子修改索引处
    private static String atom(String index, String tenant) {
        if (index.startsWith(tenant + ".") || index.startsWith("<" + tenant + ".")) {
            return index;
        }
        if (index.startsWith("+") || index.startsWith("-") || index.startsWith("<")) {
            return index.charAt(0) + tenant + "." + index.substring(1);
        } else {
            return tenant + "." + index;
        }
    }

    /**
     * 修改JsonNode
     *
     * @param parent
     * @param fieldName
     */
    public static void rewriteJson(JsonNode parent, String fieldName, String tenant, boolean nested) {
        if (nested && fieldName.contains(".")) {
            int pos = fieldName.indexOf(".");
            String field = fieldName.substring(0, pos);
            String nextField = fieldName.substring(pos + 1);
            JsonNode child = parent.get(field);
            if (child == null) {
                // no-op if not exists
                return;
            }
            if (child instanceof ArrayNode) {
                child.forEach(jsonNode -> rewriteJson(jsonNode, nextField, tenant, nested));
            } else {
                rewriteJson(parent.get(field), nextField, tenant, nested);
            }
        }

        if (parent.has(fieldName)) {
            String value = parent.get(fieldName).textValue();
            value = rewrite(value, tenant);
            ((ObjectNode) parent).put(fieldName, value);
        }
    }

    public static void rewriteJson(JsonNode parent, String fieldName, String tenant) {
        rewriteJson(parent, fieldName, tenant, false);
    }
}
