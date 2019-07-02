package org.elasticsearch.vpack.tenant;

import java.util.Arrays;
import java.util.stream.Collectors;

public class PathTenantRewriter {

    public static String[] rewrite(String[] keys, String tenant) {
        if (keys == null || keys.length == 0) {
            return keys;
        }
        String[] rewrited = new String[keys.length];
        for (int i = 0; i < keys.length; i++) {
            rewrited[i] = rewrite(keys[i], tenant);
        }
        return rewrited;
    }

    public static String rewrite(String key, String tenant) {
        if (tenant == null || tenant.trim().length() == 0) {
            return key;
        }
        if (key == null || key.trim().length() == 0 || key.equals("*") || key.equals("_all")) {
            return tenant + ".*";
        }
        return rewriteByComma(key, tenant);
    }


    private static String rewriteByComma(String key, String tenant) {
        return Arrays.stream(key.split(","))
                .map(idx -> atom(idx, tenant))
                .collect(Collectors.joining(","));
    }

    private static String atom(String key, String tenant) {
        if (key.startsWith(tenant + ".")) {
            return key;
        }
        return tenant + "." + key;
    }
}
