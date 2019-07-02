/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.vpack;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;

public class TenantContext {

    private static final String TENANT_HEADER_KEY = "AppName";
    private static final String TENANT_CONTEXT_KEY = "__" + TENANT_HEADER_KEY;

    private static final String AUTH_HEADER_KEY = "Authorization";
    private static final String AUTH_CONTEXT_KEY = "__" + "AUTH_HEADER_KEY";

    public static void process(RestRequest request, ThreadContext threadContext) {
        threadContext.putTransient(TENANT_CONTEXT_KEY, request.header(TENANT_HEADER_KEY));
        threadContext.putTransient(AUTH_CONTEXT_KEY, request.header(AUTH_HEADER_KEY));
    }

    /**
     * 获取租户
     *
     * @param threadContext
     * @return tenant
     */
    public static String tenant(ThreadContext threadContext) {
        return threadContext.getTransient(TENANT_CONTEXT_KEY);
    }

    /**
     * 获取Basic校验信息
     *
     * @param threadContext
     * @return userAuthInfo
     */
    public static String authorization(ThreadContext threadContext) {
        return threadContext.getTransient(AUTH_CONTEXT_KEY);
    }


}
