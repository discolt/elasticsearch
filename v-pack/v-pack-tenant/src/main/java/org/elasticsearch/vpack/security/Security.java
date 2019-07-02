package org.elasticsearch.vpack.security;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.vpack.TenantContext;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

public class Security {

    private volatile boolean enabled;
    private volatile String username;
    private volatile String password;

    public Security(Settings settings, ClusterService clusterService) {
        this.enabled = SCEURITY_ENABLED.get(settings);
        this.username = SCEURITY_USERNAME.get(settings);
        this.password = SCEURITY_PASSWORD.get(settings);
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(SCEURITY_ENABLED, this::setEnabled);
        clusterSettings.addSettingsUpdateConsumer(SCEURITY_USERNAME, this::setUsername);
        clusterSettings.addSettingsUpdateConsumer(SCEURITY_PASSWORD, this::setPassword);
    }

    public void doAuth(ThreadContext context) {
        if (enabled) {
            String innerCredentials = base64Encode(username, password);
            String userCredentials = TenantContext.authorization(context);
            if (!innerCredentials.equals(userCredentials)) {
                ElasticsearchSecurityException unauthorized = new ElasticsearchSecurityException("unauthorized", RestStatus.UNAUTHORIZED);
                unauthorized.addHeader("WWW-Authenticate", "Basic realm=\"please access with username and password!\"");
                throw unauthorized;
            }
        }
    }

    private void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    private void setUsername(String username) {
        this.username = username;
    }

    private void setPassword(String password) {
        this.password = password;
    }

    private static String base64Encode(String username, String password) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes(StandardCharsets.US_ASCII));
        String authHeader = "Basic " + new String(encodedAuth);
        return authHeader;
    }

    public static final Setting<Boolean> SCEURITY_ENABLED =
            Setting.boolSetting("vpack.security.enabled", false, Property.Dynamic, Property.NodeScope);

    public static final Setting<String> SCEURITY_USERNAME =
            Setting.simpleString("vpack.security.username", "admin", Property.Dynamic, Property.NodeScope);

    public static final Setting<String> SCEURITY_PASSWORD =
            Setting.simpleString("vpack.security.password", "admin", Property.Dynamic, Property.NodeScope);

    public static List<Setting<?>> getSettings() {
        return Arrays.asList(SCEURITY_ENABLED, SCEURITY_USERNAME, SCEURITY_PASSWORD);
    }

}
