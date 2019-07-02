package org.elasticsearch.vpack;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.vpack.security.Security;
import org.elasticsearch.vpack.tenant.rest.TenantRestHandler;

/**
 * Rest拦截处理
 * befores:前处理
 * procceds:过程处理
 */
public class RestHandlerChain implements RestHandler {

    private final Security security;
    private final ThreadContext threadContext;
    private final TenantRestHandler tenantRestHandler;

    public RestHandlerChain(Security security, ThreadContext threadContext, RestHandler restHandler) {
        this.threadContext = threadContext;
        this.security = security;
        this.tenantRestHandler = new TenantRestHandler(threadContext, restHandler);
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        TenantContext.process(request, threadContext);
        security.doAuth(threadContext);
        tenantRestHandler.handleRequest(request, channel, client);
    }
}
