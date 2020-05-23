package org.elasticsearch;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.query.QueryShardContext;

import java.util.Arrays;
import java.util.stream.Collectors;

public class Tenant {

    public static final Setting<String> INDEX_TENANT_SETTING =
        Setting.simpleString("index.tenant", Setting.Property.Final, Setting.Property.IndexScope);

    public static final String CONTEXT_HEADER = "AppName";

    /**
     * rewrite
     */
    public static <T> T maybeRewrite(T value, QueryShardContext context) {
        if (context == null) {
            return value;
        }
        String tenant = getTenant(context);
        return rewrite(value, tenant);
    }

    /**
     * rewrite
     */
    public static <T> T maybeRewrite(T value, ThreadContext context) {
        if (context == null) {
            return value;
        }
        String tenant = getTenant(context);
        return rewrite(value, tenant);
    }

    /**
     * revert
     */
    public static String maybeRevert(String index, ThreadContext context) {
        String tenant = getTenant(context);
        if (Strings.isEmpty(tenant)) {
            return index;
        }
        return index.startsWith(tenant) ? index.substring(tenant.length() + 1) : index;
    }

    public static String getTenant(QueryShardContext context) {
        try {
            return INDEX_TENANT_SETTING.get(context.getIndexSettings().getSettings());
        } catch (Exception e) {
            return null;
        }
    }

    public static String getTenant(ThreadContext context) {
        return context == null ? null : context.getHeader(CONTEXT_HEADER);
    }

    public static <T> T rewrite(T value, String tenant) {
        if (tenant == null) {
            return value;
        }
        if (value instanceof String) {
            value = (T) rewrite((String) value, tenant);
        }
        return value;
    }

    public static String rewrite(String index, String tenant) {
        if (Strings.isEmpty(tenant) || Strings.isEmpty(index)) {
            return index;
        }
        if (index.equals("*") || index.equals("_all")) {
            return tenant + ".*";
        }
        return Arrays.stream(index.split(","))
            .map(idx -> atomRewrite(idx, tenant))
            .collect(Collectors.joining(","));
    }

    private static String atomRewrite(String index, String tenant) {
        if (index.startsWith(tenant + ".") || index.startsWith("<" + tenant + ".")) {
            return index;
        }
        if (index.startsWith("+") || index.startsWith("-") || index.startsWith("<")) {
            return index.charAt(0) + tenant + "." + index.substring(1);
        } else {
            return tenant + "." + index;
        }
    }

}

