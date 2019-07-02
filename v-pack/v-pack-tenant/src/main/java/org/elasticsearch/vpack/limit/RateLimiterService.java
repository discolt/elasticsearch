package org.elasticsearch.vpack.limit;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.vpack.limit.action.delete.DeleteRateLimiterRequest;
import org.elasticsearch.vpack.limit.action.put.PutRateLimiterRequest;

import java.util.*;

public class RateLimiterService implements ClusterStateApplier {

    private volatile boolean enabled;
    private final ClusterService clusterService;
    private RateLimiterProvider rateLimiterProvider;

    @Inject
    public RateLimiterService(ClusterService clusterService) {
        this.clusterService = clusterService;
        this.enabled = LIMITER_ENABLED.get(clusterService.getSettings());
        this.rateLimiterProvider = new RateLimiterProvider(clusterService);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(LIMITER_ENABLED, newValue -> this.enabled = newValue);
    }

    public <Request extends ActionRequest> void acquire(String action, String tenant, Request request) {
        if (enabled) {
            List<RateLimiterProvider.TenantRateLimiter> rateLimiters = rateLimiterProvider.retrieve(action, tenant);
            if (rateLimiters != null && !rateLimiters.isEmpty()) {
                rateLimiters.forEach(e -> e.acquire(request));
            }
        }
    }

    /**
     * 添加限流规则
     *
     * @param request
     * @param listener
     */
    public void putLimiter(PutRateLimiterRequest request, ActionListener<AcknowledgedResponse> listener) {
        validateLimiter(request);
        clusterService.submitStateUpdateTask("put-limiter-" + request.getTenant(),
                new AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {
                    @Override
                    protected AcknowledgedResponse newResponse(boolean acknowledged) {
                        return new AcknowledgedResponse(acknowledged);
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return innerPut(request, currentState);
                    }
                });
    }

    /**
     * 校验添加限流规则
     *
     * @param request
     */
    void validateLimiter(PutRateLimiterRequest request) {
        Map<String, Object> limitConfig = XContentHelper.convertToMap(request.getSource(), false, request.getXContentType()).v2();
        rateLimiterProvider.validate(limitConfig);
    }

    /**
     * 删除限流规则
     *
     * @param request
     * @param listener
     */
    public void delete(DeleteRateLimiterRequest request, ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask("delete-limiter-" + request.getTenant(),
                new AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {
                    @Override
                    protected AcknowledgedResponse newResponse(boolean acknowledged) {
                        return new AcknowledgedResponse(acknowledged);
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return innerDelete(request, currentState);
                    }
                });
    }


    static ClusterState innerPut(PutRateLimiterRequest request, ClusterState currentState) {
        RateLimiterMetadata currentLimiterMetadata = currentState.metaData().custom(RateLimiterMetadata.TYPE);
        Map<String, RateLimiterConfiguration> rateLimiters;
        if (currentLimiterMetadata != null) {
            rateLimiters = new HashMap(currentLimiterMetadata.getRateLimiters());
        } else {
            rateLimiters = new HashMap<>();
        }
        rateLimiters.put(request.getTenant(), new RateLimiterConfiguration(request.getTenant(), request.getSource(), request.getXContentType()));
        ClusterState.Builder newState = ClusterState.builder(currentState);
        newState.metaData(MetaData.builder(currentState.getMetaData())
                .putCustom(RateLimiterMetadata.TYPE, new RateLimiterMetadata(rateLimiters))
                .build());
        return newState.build();
    }

    static ClusterState innerDelete(DeleteRateLimiterRequest request, ClusterState currentState) {
        RateLimiterMetadata currentLimiterMetadata = currentState.metaData().custom(RateLimiterMetadata.TYPE);
        if (currentLimiterMetadata == null) {
            return currentState;
        }
        Map<String, RateLimiterConfiguration> rateLimiters = currentLimiterMetadata.getRateLimiters();
        Set<String> toRemove = new HashSet<>();
        for (String limiterKey : rateLimiters.keySet()) {
            if (Regex.simpleMatch(request.getTenant(), limiterKey)) {
                toRemove.add(limiterKey);
            }
        }
        if (toRemove.isEmpty() && Regex.isMatchAllPattern(request.getTenant()) == false) {
            throw new ResourceNotFoundException("limiter [{}] is missing", request.getTenant());
        } else if (toRemove.isEmpty()) {
            return currentState;
        }
        final Map<String, RateLimiterConfiguration> limitersCopy = new HashMap<>(rateLimiters);
        for (String key : toRemove) {
            limitersCopy.remove(key);
        }
        ClusterState.Builder newState = ClusterState.builder(currentState);
        newState.metaData(MetaData.builder(currentState.getMetaData())
                .putCustom(RateLimiterMetadata.TYPE, new RateLimiterMetadata(limitersCopy))
                .build());
        return newState.build();
    }

    /**
     * 获取限流规则列表
     *
     * @param clusterState
     * @param ids
     * @return
     */
    public static List<RateLimiterConfiguration> getLimiters(ClusterState clusterState, String... ids) {
        RateLimiterMetadata rateLimiterMetadata = clusterState.getMetaData().custom(RateLimiterMetadata.TYPE);
        return innerGetLimiters(rateLimiterMetadata, ids);
    }

    static List<RateLimiterConfiguration> innerGetLimiters(RateLimiterMetadata limiterMetadata, String... tenants) {
        if (limiterMetadata == null) {
            return Collections.emptyList();
        }

        // if we didn't ask for _any_ ID, then we get them all (this is the same as if they ask for '*')
        if (tenants.length == 0) {
            return new ArrayList<>(limiterMetadata.getRateLimiters().values());
        }

        List<RateLimiterConfiguration> result = new ArrayList<>(tenants.length);
        for (String id : tenants) {
            if (Regex.isSimpleMatchPattern(id)) {
                for (Map.Entry<String, RateLimiterConfiguration> entry : limiterMetadata.getRateLimiters().entrySet()) {
                    if (Regex.simpleMatch(id, entry.getKey())) {
                        result.add(entry.getValue());
                    }
                }
            } else {
                RateLimiterConfiguration limiter = limiterMetadata.getRateLimiters().get(id);
                if (limiter != null) {
                    result.add(limiter);
                }
            }
        }
        return result;
    }


    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        ClusterState state = event.state();
        innerUpdateLimiters(event.previousState(), state);
    }

    private void innerUpdateLimiters(ClusterState previousState, ClusterState state) {
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        RateLimiterMetadata limiterMetadata = state.getMetaData().custom(RateLimiterMetadata.TYPE);
        RateLimiterMetadata previousLimiterMetadata = previousState.getMetaData().custom(RateLimiterMetadata.TYPE);
        if (Objects.equals(limiterMetadata, previousLimiterMetadata)) {
            return;
        }

        RateLimiterProvider rateLimiterProvider = new RateLimiterProvider(clusterService);
        List<ElasticsearchParseException> exceptions = new ArrayList<>();
        for (RateLimiterConfiguration limiterConf : limiterMetadata.getRateLimiters().values()) {
            try {
                rateLimiterProvider.addLimiter(
                        limiterConf.getTenant(),
                        limiterConf.getConfigAsMap());
            } catch (ElasticsearchParseException e) {
                exceptions.add(e);
            } catch (Exception e) {
                ElasticsearchParseException parseException = new ElasticsearchParseException(
                        "Error updating limiter with tenant [" + limiterConf.getTenant() + "]", e);
                exceptions.add(parseException);
            }
        }
        this.rateLimiterProvider = rateLimiterProvider;
        ExceptionsHelper.rethrowAndSuppress(exceptions);
    }

    public static final Setting<Boolean> LIMITER_ENABLED =
            Setting.boolSetting("vpack.limiter.enabled", true, Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static List<Setting<?>> getSettings() {
        return Arrays.asList(LIMITER_ENABLED);
    }
}
