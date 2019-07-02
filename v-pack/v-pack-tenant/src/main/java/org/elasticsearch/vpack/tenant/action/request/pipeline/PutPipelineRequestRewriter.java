package org.elasticsearch.vpack.tenant.action.request.pipeline;

import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.action.ActionRequestRewriter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PutPipelineRequestRewriter implements ActionRequestRewriter<PutPipelineRequest> {

    @Override
    public PutPipelineRequest rewrite(PutPipelineRequest request, String tenant) {
        String id = IndexTenantRewriter.rewrite(request.getId(), tenant);
        Map<String, Object> pipelineConfig = XContentHelper.convertToMap(request.getSource(), false, request.getXContentType()).v2();
        XContentBuilder builder = null;
        if (pipelineConfig.containsKey("processors")) {
            List<Map<String, Map>> processors = (List) pipelineConfig.get("processors");
            processors.forEach(e -> {
                e.forEach((k, v) -> {
                    // 替换用户pipeline-id
                    if (k.equals("pipeline")) {
                        String name = (String) v.get("name");
                        if (name != null) {
                            Map replaced = Collections.singletonMap("name", IndexTenantRewriter.rewrite(name, tenant));
                            e.put(k, replaced);
                        }
                    }
                    // 替换 set _index
                    if (k.equals("set")) {
                        String field = (String) v.get("field");
                        if (field != null && field.equals("_index")) {
                            v.put("value", IndexTenantRewriter.rewrite((String) v.get("value"), tenant));
                        }
                    }
                    // 替换 date_index_name 里的 index_name_prefix
                    if (k.equals("date_index_name")) {
                        String index_name_prefix = (String) v.get("index_name_prefix");
                        if (index_name_prefix != null && index_name_prefix.trim().length() > 0) {
                            v.put("index_name_prefix", IndexTenantRewriter.rewrite(index_name_prefix, tenant));
                        } else {
                            v.put("index_name_prefix", tenant + ".");
                        }
                    }
                });
            });
            try {
                builder = XContentFactory.contentBuilder(XContentType.JSON);
                builder.map(pipelineConfig);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        BytesReference source = builder == null ? request.getSource() : BytesReference.bytes(builder);
        PutPipelineRequest replaced = new PutPipelineRequest(id, source, request.getXContentType());
        return replaced;
    }
}
