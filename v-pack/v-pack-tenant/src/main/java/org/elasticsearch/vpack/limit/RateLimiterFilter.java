package org.elasticsearch.vpack.limit;


import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.vpack.TenantContext;


public class RateLimiterFilter implements ActionFilter {

    private final ThreadContext threadContext;
    private final RateLimiterService rateLimiterService;

    public RateLimiterFilter(ThreadContext threadContext, RateLimiterService rateLimiterService) {
        this.threadContext = threadContext;
        this.rateLimiterService = rateLimiterService;
    }

    @Override
    public int order() {
        return 0;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
            Task task, String action, Request request, ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {
        String tenant = TenantContext.tenant(threadContext);
        rateLimiterService.acquire(action, tenant, request);
        chain.proceed(task, action, request, listener);
    }
}
