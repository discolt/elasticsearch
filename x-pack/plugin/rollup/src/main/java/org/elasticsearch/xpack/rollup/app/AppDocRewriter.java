package org.elasticsearch.xpack.rollup.app;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.rollup.RollupFeatureSet;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class AppDocRewriter {

    private static final Logger logger = Logger.getLogger(AppDocRewriter.class.getName());

    private static final int EXPIRY_TIME_IN_MILLIS = 1000 * 60 * 30;
    private static final String FIELD_ACCOUNT_ID = "accountId";
    private static final String FIELD_ACCOUNT_TYPE = "accountType";
    private static final String FIELD_APP_NAME = "appName";
    private static RestClient _client;
    private static Cache cache = new Cache();

    private static RestClient client() {
        if (_client != null) {
            return _client;
        }
        RestClientBuilder builder = RestClient.builder(new HttpHost(HttpHost.create(RollupFeatureSet.CONSOLE_HOST)))
            .setHttpClientConfigCallback(httpClientBuilder -> {
                final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(RollupFeatureSet.CONSOLE_USERNAME, RollupFeatureSet.CONSOLE_PASSWORD));
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            });
        _client = builder.build();
        return _client;
    }

    public static boolean enabled() {
        return !hasEmpty(RollupFeatureSet.CONSOLE_HOST);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, String> getAccountAppname(String index) {
        Map<String, String> result = (Map<String, String>) cache.get("__index:" + index);
        if (result == null) {
            try {
                Map<String, String> data = httpGetAccountAppname(index);
                if (data != null) {
                    cache.put("__index:" + index, data, EXPIRY_TIME_IN_MILLIS);
                    return data;
                }
            } catch (Exception e) {
                logger.error(e);
                return null;
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, String> getAccount(String appName) {
        Map<String, String> result = (Map<String, String>) cache.get("__app:" + appName);
        if (result == null) {
            try {
                Map<String, String> data = httpGetAccount(appName);
                if (data != null) {
                    cache.put("__app:" + appName, data, EXPIRY_TIME_IN_MILLIS);
                    return data;
                }
            } catch (IOException e) {
                logger.error(e);
                return null;
            }
        }
        return result;
    }


    @SuppressWarnings("unchecked")
    private static Map<String, String> httpGetAccountAppname(String index) throws IOException {
        Request request = new Request("GET", "/open/api/get_account_appname/cloud/" + index);
        Response response = client().performRequest(request);
        Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), false);
        Map<String, Object> data = (Map<String, Object>) map.get("data");
        assetNotNull(data, "data require");
        String appName = (String) data.get("appName");
        Map<String, Object> account = (Map<String, Object>) data.get("account");
        assetNotNull(account, "account miss");
        assetNotNull(appName, "appName miss");
        Map<String, String> result = new HashMap<>();
        result.put(FIELD_ACCOUNT_ID, (String) account.get("accountId"));
        result.put(FIELD_ACCOUNT_TYPE, (String) account.get("accountType"));
        result.put(FIELD_APP_NAME, appName);
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> httpGetAccount(String appName) throws IOException {
        Request request = new Request("GET", "/open/api/get_account/cloud/" + appName);
        Response response = client().performRequest(request);
        Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), false);
        Map<String, Object> data = (Map<String, Object>) map.get("data");
        assetNotNull(data, "data require");
        if (data == null || data.size() != 2) {
            return Collections.emptyMap();
        }
        Map<String, String> result = new HashMap<>();
        result.put("accountId", (String) data.get("accountId"));
        result.put("accountType", (String) data.get("accountType"));
        return result;
    }

    public static void rewrite(Map<String, Object> doc, String field, Object value) {
        if (field.equals("index_stats.index.terms")) {
            String index = (String) value;
            if (filterIndex(index)) {
                Map<String, String> appData = getAccountAppname(index);
                if (appData == null) {
                    return;
                }
                String accountId = appData.get(FIELD_ACCOUNT_ID);
                String accountType = appData.get(FIELD_ACCOUNT_TYPE);
                String appName = appData.get(FIELD_APP_NAME);
                if (hasEmpty(accountId, accountType, appName)) {
                    return;
                }
                doc.put(FIELD_ACCOUNT_ID, accountId);
                doc.put(FIELD_ACCOUNT_TYPE, accountType);
                doc.put(FIELD_APP_NAME, appName);
                return;
            }
        }

        if (field.equals("nginx.access.app_name.terms")) {
            String appName = (String) value;
            if (filterAppname(appName)) {
                Map<String, String> data = getAccount(appName);
                if (data == null) {
                    return;
                }
                Map<String, String> accountData = getAccount(appName);
                if (accountData.size() != 2) {
                    return;
                }
                String accountId = accountData.get(FIELD_ACCOUNT_ID);
                String accountType = accountData.get(FIELD_ACCOUNT_TYPE);
                if (hasEmpty(accountId, accountType)) {
                    return;
                }
                doc.put(FIELD_ACCOUNT_ID, accountId);
                doc.put(FIELD_ACCOUNT_TYPE, accountType);
            }
        }

    }

    private static boolean filterIndex(String index) {
        if (index == null || index.trim().length() == 0) {
            return false;
        }
        if (index.startsWith(".")) {
            return false;
        }
        if (index.startsWith("rollup")) {
            return false;
        }
        String regex = ".*[.]+.*";
        return index.matches(regex);
    }

    private static String APPNAME_PATTERN = "^[a-z][a-zA-Z0-9_]{2,80}";

    private static boolean filterAppname(String appname) {
        if (appname == null || appname.trim().length() == 0) {
            return false;
        }
        return Pattern.matches(APPNAME_PATTERN, appname);
    }

    private static void assetNotNull(Object object, String msg) {
        if (object == null) {
            throw new RuntimeException(msg);
        }
    }

    private static boolean hasEmpty(String... strings) {
        boolean b = false;
        for (String s : strings) {
            b = b || (s == null || s.trim().length() == 0);
        }
        return b;
    }

}
