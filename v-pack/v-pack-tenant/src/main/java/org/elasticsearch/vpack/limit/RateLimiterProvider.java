package org.elasticsearch.vpack.limit;

import com.google.common.util.concurrent.RateLimiter;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class RateLimiterProvider {

    private static final String GLOBAL_TENANT = "_global";
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public RateLimiterProvider(ClusterService clusterService) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = new IndexNameExpressionResolver(clusterService.getSettings());
    }

    private Map<String, List<TenantRateLimiter>> providers = new HashMap<>();

    /**
     * 添加租户限流规则
     *
     * @param tenant 租户
     * @param config 规则配置
     */
    public void addLimiter(String tenant, Map<String, Object> config) {
        config.forEach((k, v) -> parseAndAdd(tenant, k, v));
    }

    private void parseAndAdd(String tenant, String field, Object threshold) {
        TenantRateLimiter tenantRateLimiter = factoryRateLimiter(tenant, field, threshold);
        if (!providers.containsKey(tenantRateLimiter.key())) {
            providers.put(tenantRateLimiter.key(), new ArrayList());
        }
        providers.get(tenantRateLimiter.key()).add(tenantRateLimiter);
    }

    /**
     * 获取限流器
     * 先获取租户级限流器， 如果没有再获取全局限流器。
     *
     * @param action
     * @param tenant
     * @return
     */
    public List<TenantRateLimiter> retrieve(String action, String tenant) {
        List<TenantRateLimiter> limiters = providers.get(keys(action, tenant));
        if (limiters == null || limiters.isEmpty()) {
            return providers.get(keys(action, GLOBAL_TENANT));
        } else {
            return limiters;
        }
    }

    /**
     * 校验限流规则
     *
     * @param config
     */
    public void validate(Map<String, Object> config) {
        config.forEach((k, v) -> factoryRateLimiter("", k, v));
    }


    /**
     * 租户限流器
     *
     * @param <Request>
     */
    abstract static class TenantRateLimiter<Request extends ActionRequest> {

        private boolean blocked = false;
        private RateLimiter rateLimiter;
        private String tenant;

        public TenantRateLimiter(String tenant, Object threshold) {
            this.tenant = tenant;
            long throttle = value(threshold);
            if (throttle < 0) {
                blocked = true;
            } else {
                this.rateLimiter = RateLimiter.create(throttle);
            }
        }

        /**
         * 限流阀值
         */
        abstract String field();

        /**
         * 挂接的ActionName
         */
        abstract String action();

        /**
         * 计算消耗由子类实现
         */
        abstract int consumed(Request request);

        /**
         * 是否应用在Request上，通常有index子集限流的需要自己实现
         */
        protected boolean apply(Request request) {
            return true;
        }

        /**
         * 放到Provider Map 的key
         */
        public String key() {
            return keys(action(), tenant);
        }

        /**
         * 实际限流动作
         */
        public final void acquire(Request request) {
            if (apply(request)) {
                if (blocked) {
                    throw new RateLimitedException("request blocked, tenant[{}] limited by [{}] ", tenant, field());
                }
                int consumed = consumed(request);
                if (!rateLimiter.tryAcquire(consumed)) {
                    throw new RateLimitedException("request rejected, tenant[{}] limited by [{}:{}] ", tenant, field(), rateLimiter.getRate());
                }
            }

        }
    }

    private static long value(Object val) {
        if (val instanceof Integer) {
            return (int) val;
        } else if (val instanceof Long) {
            return (long) val;
        } else if (val instanceof String) {
            try {
                return ByteSizeValue.parseBytesSizeValue(String.valueOf(val), "value").getBytes();
            } catch (Exception e) {
            }
            return Long.valueOf(String.valueOf(val));
        }
        throw new ElasticsearchParseException("failed to parse value [{}] ", val);
    }

    private static String keys(String action, String tenant) {
        return action + ":" + tenant;
    }

    /**
     * TenantRateLimiter Factory  .
     *
     * @param tenant
     * @param field
     * @param threshold
     * @return
     */
    public TenantRateLimiter factoryRateLimiter(String tenant, String field, Object threshold) {

        /*
         * 写入流量
         */
        if (field.equals("index.max_bytes")) {
            return new TenantRateLimiter<BulkRequest>(tenant, threshold) {
                @Override
                String field() {
                    return field;
                }

                @Override
                String action() {
                    return BulkAction.NAME;
                }

                @Override
                int consumed(BulkRequest request) {
                    return (int) request.estimatedSizeInBytes();
                }
            };
        }

        /*
         * 查询次数
         */
        if (field.startsWith("search.max_times")) {

            return new TenantRateLimiter<SearchRequest>(tenant, threshold) {
                @Override
                String field() {
                    return field;
                }

                @Override
                String action() {
                    return SearchAction.NAME;
                }

                @Override
                protected boolean apply(SearchRequest request) {
                    int i = StringUtils.ordinalIndexOf(field, ".", 2);
                    String expression = i == StringUtils.INDEX_NOT_FOUND ? "*" : field.substring(i + 1);
                    expression = tenant.equals(GLOBAL_TENANT) ? expression : IndexTenantRewriter.rewrite(expression, tenant);
                    return indicesNameMatcher.apply(request, expression);
                }

                @Override
                int consumed(SearchRequest request) {
                    return 1;
                }
            };
        }
        throw new UnsupportedOperationException("unkown-limit-define:" + field);
    }

    private BiFunction<IndicesRequest, String, Boolean> indicesNameMatcher = new BiFunction<IndicesRequest, String, Boolean>() {
        @Override
        public Boolean apply(IndicesRequest indicesRequest, String expression) {
            String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterService.state(), indicesRequest);
            for (String index : concreteIndices) {
                if (Regex.simpleMatch(index, expression)) {
                    return true;
                }
            }
            return false;
        }
    };


}
