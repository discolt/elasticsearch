
package org.elasticsearch.vpack;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.*;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.vpack.limit.RateLimiterFilter;
import org.elasticsearch.vpack.limit.RateLimiterMetadata;
import org.elasticsearch.vpack.limit.RateLimiterService;
import org.elasticsearch.vpack.limit.action.delete.DeleteRateLimiterAction;
import org.elasticsearch.vpack.limit.action.delete.DeleteRateLimiterTransportAction;
import org.elasticsearch.vpack.limit.action.get.GetRateLimiterAction;
import org.elasticsearch.vpack.limit.action.get.GetRateLimiterTransportAction;
import org.elasticsearch.vpack.limit.action.put.PutRateLimiterAction;
import org.elasticsearch.vpack.limit.action.put.PutRateLimiterTransportAction;
import org.elasticsearch.vpack.limit.rest.RestDeleteRateLimiterAction;
import org.elasticsearch.vpack.limit.rest.RestGetRateLimiterAction;
import org.elasticsearch.vpack.limit.rest.RestPutRateLimiterAction;
import org.elasticsearch.vpack.security.Security;
import org.elasticsearch.vpack.tenant.action.TenantActionFilter;
import org.elasticsearch.vpack.tenant.field.IndexFieldMapper;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.*;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public class TenantPlugin extends Plugin implements ActionPlugin, MapperPlugin {

    private final SetOnce<Settings> settingsSetOnce = new SetOnce<>();
    private final SetOnce<ClusterService> clusterServiceSetOnce = new SetOnce<>();
    private final SetOnce<TenantActionFilter> tenantActionFilterSetOnce = new SetOnce<>();
    private final SetOnce<RateLimiterFilter> rateLimiterFilterSetOnce = new SetOnce<>();

    public TenantPlugin() {
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
                new ActionHandler<>(PutRateLimiterAction.INSTANCE, PutRateLimiterTransportAction.class),
                new ActionHandler<>(DeleteRateLimiterAction.INSTANCE, DeleteRateLimiterTransportAction.class),
                new ActionHandler<>(GetRateLimiterAction.INSTANCE, GetRateLimiterTransportAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(
                new RestPutRateLimiterAction(settings, restController),
                new RestDeleteRateLimiterAction(settings, restController),
                new RestGetRateLimiterAction(settings, restController)
        );

    }

    @Override
    public UnaryOperator<RestHandler> getRestHandlerWrapper(ThreadContext threadContext) {
        Security security = new Security(settingsSetOnce.get(), clusterServiceSetOnce.get());
        return handler -> new RestHandlerChain(security, threadContext, handler);
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        return Arrays.asList(rateLimiterFilterSetOnce.get(), tenantActionFilterSetOnce.get());
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {

        settingsSetOnce.set(client.settings());
        clusterServiceSetOnce.set(clusterService);
        tenantActionFilterSetOnce.set(new TenantActionFilter(threadPool.getThreadContext()));
        RateLimiterService rateLimiterService = new RateLimiterService(clusterService);
        clusterService.addLowPriorityApplier(rateLimiterService);
        rateLimiterFilterSetOnce.set(new RateLimiterFilter(threadPool.getThreadContext(), rateLimiterService));
        return Arrays.asList(rateLimiterService);
    }

    @Override
    public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
        Map<String, MetadataFieldMapper.TypeParser> mappers = new LinkedHashMap<>();
        mappers.put(IndexFieldMapper.NAME, new IndexFieldMapper.TypeParser());
        return Collections.unmodifiableMap(mappers);
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList();
        settings.addAll(Security.getSettings());
        settings.addAll(RateLimiterService.getSettings());
        return settings;
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        registerMetaDataCustom(entries, RateLimiterMetadata.TYPE, RateLimiterMetadata::new, RateLimiterMetadata::readDiffFrom);
        return entries;
    }

    private static <T extends MetaData.Custom> void registerMetaDataCustom(
            List<NamedWriteableRegistry.Entry> entries, String name,
            Writeable.Reader<? extends T> reader,
            Writeable.Reader<NamedDiff> diffReader) {
        registerCustom(entries, MetaData.Custom.class, name, reader, diffReader);
    }

    private static <T extends NamedWriteable> void registerCustom(
            List<NamedWriteableRegistry.Entry> entries, Class<T> category, String name,
            Writeable.Reader<? extends T> reader, Writeable.Reader<NamedDiff> diffReader) {
        entries.add(new NamedWriteableRegistry.Entry(category, name, reader));
        entries.add(new NamedWriteableRegistry.Entry(NamedDiff.class, name, diffReader));
    }

    /**
     * 支持重启后恢复 RateLimiterMetadata
     *
     * @return
     */
    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return Arrays.asList(
                new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(RateLimiterMetadata.TYPE), RateLimiterMetadata::fromXContent)
        );
    }

}
