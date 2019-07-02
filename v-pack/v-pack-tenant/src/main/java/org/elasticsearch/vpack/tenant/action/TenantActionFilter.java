package org.elasticsearch.vpack.tenant.action;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresAction;
import org.elasticsearch.action.admin.indices.shrink.ResizeAction;
import org.elasticsearch.action.admin.indices.shrink.ShrinkAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.upgrade.get.UpgradeStatusAction;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesAction;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.ingest.DeletePipelineAction;
import org.elasticsearch.action.ingest.GetPipelineAction;
import org.elasticsearch.action.ingest.PutPipelineAction;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.termvectors.MultiTermVectorsAction;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.vpack.TenantContext;
import org.elasticsearch.vpack.tenant.action.request.*;
import org.elasticsearch.vpack.tenant.action.request.indices.*;
import org.elasticsearch.vpack.tenant.action.request.pipeline.DeletePipelineRequestRewriter;
import org.elasticsearch.vpack.tenant.action.request.pipeline.GetPipelineRequestRewriter;
import org.elasticsearch.vpack.tenant.action.request.pipeline.PutPipelineRequestRewriter;
import org.elasticsearch.vpack.tenant.action.request.script.DeleteStoredScriptRequestRewriter;
import org.elasticsearch.vpack.tenant.action.request.script.GetStoredScriptRequestRewriter;
import org.elasticsearch.vpack.tenant.action.request.script.PutStoredScriptRequestRewriter;
import org.elasticsearch.vpack.tenant.action.request.state.ClusterStateRequestRewriter;
import org.elasticsearch.vpack.tenant.action.request.template.DeleteIndexTemplateRequestRewriter;
import org.elasticsearch.vpack.tenant.action.request.template.GetIndexTemplatesRequestRewriter;
import org.elasticsearch.vpack.tenant.action.request.template.PutIndexTemplateRequestRewriter;

import java.util.HashMap;
import java.util.Map;

public class TenantActionFilter implements ActionFilter {

    private final ThreadContext threadContext;
    private Map<String, ActionRequestRewriter> requestHandlers = new HashMap<>();

    public TenantActionFilter(ThreadContext threadContext) {
        super();
        this.threadContext = threadContext;
        // bulk
        this.requestHandlers.put(BulkAction.NAME, new BulkRequestRewriter());
        // search
        this.requestHandlers.put(SearchAction.NAME, new SearchRequestRewriter());
        this.requestHandlers.put(MultiSearchAction.NAME, new MultiSearchRequestRewriter());
        this.requestHandlers.put(MultiGetAction.NAME, new MultiGetRequestRewriter());
        this.requestHandlers.put(MultiTermVectorsAction.NAME, new MultiTermVectorsRequestRewriter());
        // alias
        this.requestHandlers.put(IndicesAliasesAction.NAME, new IndicesAliasesRequestRewriter());
        // reindex
        this.requestHandlers.put(ReindexAction.NAME, new ReindexRequestRewriter());
        // indices
        this.requestHandlers.put(GetIndexAction.NAME, new GetIndexRequestRewriter());
        this.requestHandlers.put(CreateIndexAction.NAME, new CreateIndexRequestRewriter());
        this.requestHandlers.put(GetMappingsAction.NAME, new GetMappingsRequestRewriter());
        this.requestHandlers.put(GetAliasesAction.NAME, new GetAliasesRequestRewriter());
        // template
        this.requestHandlers.put(PutIndexTemplateAction.NAME, new PutIndexTemplateRequestRewriter());
        this.requestHandlers.put(GetIndexTemplatesAction.NAME, new GetIndexTemplatesRequestRewriter());
        this.requestHandlers.put(DeleteIndexTemplateAction.NAME, new DeleteIndexTemplateRequestRewriter());
        // pipeline
        this.requestHandlers.put(PutPipelineAction.NAME, new PutPipelineRequestRewriter());
        this.requestHandlers.put(GetPipelineAction.NAME, new GetPipelineRequestRewriter());
        this.requestHandlers.put(UpdateSettingsAction.NAME, new UpdateSettingsRequestRewriter());
        this.requestHandlers.put(DeletePipelineAction.NAME, new DeletePipelineRequestRewriter());
        // script
        this.requestHandlers.put(PutStoredScriptAction.NAME, new PutStoredScriptRequestRewriter());
        this.requestHandlers.put(GetStoredScriptAction.NAME, new GetStoredScriptRequestRewriter());
        this.requestHandlers.put(DeleteStoredScriptAction.NAME, new DeleteStoredScriptRequestRewriter());
        // stats
        this.requestHandlers.put(ClusterStateAction.NAME, new ClusterStateRequestRewriter());
        // settings
        this.requestHandlers.put(GetSettingsAction.NAME, new GetSettingsRequestRewriter());
        // scroll
        this.requestHandlers.put(ClearScrollAction.NAME, new ClearScrollRequestRewriter());
        // rollover
        this.requestHandlers.put(RolloverAction.NAME, new RolloverRequestRewriter());
        // shrink
        this.requestHandlers.put(ShrinkAction.NAME, new ResizeRequestRewriter());
        this.requestHandlers.put(ResizeAction.NAME, new ResizeRequestRewriter());
        // field caps
        this.requestHandlers.put(FieldCapabilitiesAction.NAME, new FieldCapabilitiesIndexRequestRewriter());

        // broadcast
        BroadcastRequestRewriter broadcastRequestRewriter = new BroadcastRequestRewriter();
        String[] broadcastActions = new String[]{
                ClearIndicesCacheAction.NAME,
                FlushAction.NAME,
                ForceMergeAction.NAME,
                IndicesStatsAction.NAME,
                RecoveryAction.NAME,
                RefreshAction.NAME,
                SyncedFlushAction.NAME,
                UpgradeAction.NAME,
                UpgradeStatusAction.NAME,
                ValidateQueryAction.NAME
        };
        for (String broadcastAction : broadcastActions) {
            this.requestHandlers.put(broadcastAction, broadcastRequestRewriter);
        }

        // 禁止的操作
        ActionRequestRewriter forbidden = (request, content) -> {
            throw new ElasticsearchSecurityException("operation forbidden", RestStatus.FORBIDDEN);
        };
        this.requestHandlers.put(IndicesShardStoresAction.NAME, forbidden);
    }

    @Override
    public int order() {
        return 9;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
            Task task, String action, Request request,
            ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {

        String tenant = TenantContext.tenant(threadContext);
        if (tenant == null) {
            chain.proceed(task, action, request, listener);
            return;
        }

        // rewrite action request
        ActionRequestRewriter<Request> handler = requestHandlers.get(action);
        if (handler != null) {
            request = handler.rewrite(request, tenant);
        }

        // wrap failure handle
        ActionListener listenerWrapper = ActionListener.wrap(o -> listener.onResponse((Response) o), e -> {
            // hidden ip-address
            listener.onFailure(e instanceof RemoteTransportException ?
                    new RemoteTransportException(e.getMessage().replaceAll("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}:\\d{1,5}", ""), e)
                    : e);
        });

        // chain process
        chain.proceed(task, action, request, listenerWrapper);
    }

}
