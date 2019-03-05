/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.rollup.RollupFeatureSetUsage;

import java.util.Map;

public class RollupFeatureSet implements XPackFeatureSet {

    public static String CONSOLE_HOST;
    public static String CONSOLE_USERNAME;
    public static String CONSOLE_PASSWORD;
    private final boolean enabled;
    private final XPackLicenseState licenseState;

    @Inject
    public RollupFeatureSet(Settings settings, @Nullable XPackLicenseState licenseState) {
        this.enabled = XPackSettings.ROLLUP_ENABLED.get(settings);
        this.licenseState = licenseState;
        CONSOLE_HOST = Rollup.ROLLUP_CONSOLE_HOST.get(settings);
        CONSOLE_USERNAME = Rollup.ROLLUP_CONSOLE_USERNAME.get(settings);
        CONSOLE_PASSWORD = Rollup.ROLLUP_CONSOLE_PASSWORD.get(settings);
    }

    @Override
    public String name() {
        return XPackField.ROLLUP;
    }

    @Override
    public String description() {
        return "Time series pre-aggregation and rollup";
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.isRollupAllowed();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    public String consoleUri() {
        return CONSOLE_HOST;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<XPackFeatureSet.Usage> listener) {
        // TODO expose the currently running rollup tasks on this node?  Unclear the best way to do that
        listener.onResponse(new RollupFeatureSetUsage(available(), enabled()));
    }


}
