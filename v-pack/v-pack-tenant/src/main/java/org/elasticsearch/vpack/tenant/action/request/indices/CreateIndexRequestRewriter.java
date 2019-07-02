package org.elasticsearch.vpack.tenant.action.request.indices;

import org.elasticsearch.action.AliasesRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.index.IndexSettings.*;

/**
 * 建索引时加上index.tenant，标识租户信息。
 */
public class CreateIndexRequestRewriter implements ActionRequestRewriter<CreateIndexRequest> {

    @Override
    public CreateIndexRequest rewrite(CreateIndexRequest request, String tenant) {
        rebuild(request, tenant);
        return request;
    }

    public static void rebuild(CreateIndexRequest request, String tenant) {
        Settings.Builder builder = Settings.builder();
        Settings settings = request.settings();
        builder.put(settings);
        builder.put("index.tenant", tenant);
        // override index.default_pipeline
        String pipeline = DEFAULT_PIPELINE.get(settings);
        if (pipeline != null && !IngestService.NOOP_PIPELINE_NAME.equals(pipeline)) {
            builder.put("index.default_pipeline", IndexTenantRewriter.rewrite(pipeline, tenant));
        }
        // alias
        if (request.aliases() != null && request.aliases().size() > 0) {
            Set<Alias> copy = request.aliases().stream().collect(Collectors.toSet());
            request.aliases().clear();
            copy.forEach((alias) -> {
                Alias replaced = new Alias(IndexTenantRewriter.rewrite(alias.name(), tenant));
                replaced.writeIndex(alias.writeIndex());
                replaced.searchRouting(alias.searchRouting());
                replaced.indexRouting(alias.indexRouting());
                replaced.filter(alias.filter());
                request.alias(replaced);
            });
        }
        request.settings(builder);
        request.index(IndexTenantRewriter.rewrite(request.index(), tenant));
    }

}
