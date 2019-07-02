package org.elasticsearch.vpack.xdcr;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;

import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;

public class ActionFilterPlugin implements ActionFilter {
    @Override
    public int order() {
        return 1;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(Task task, String action, Request request, ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {
        if (CreateIndexAction.NAME.equals(action)) {
            CreateIndexRequest createIndexRequest = (CreateIndexRequest) request;
            if (INDEX_SOFT_DELETES_SETTING.get(createIndexRequest.settings()) == true) {
                Settings.Builder builder = Settings.builder();
                Settings settings = createIndexRequest.settings();
                builder.put(settings);
                builder.put("index.soft_deletes.retention.operations", "1000000");
                createIndexRequest.settings(builder);
            }
            request = (Request) createIndexRequest;
        }
        chain.proceed(task, action, request, listener);
    }
}
