package org.elasticsearch.vpack.xdcr.action.index;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.vpack.xdcr.utils.Actions;

import java.io.IOException;

public class IndexLevelRequest extends AcknowledgedRequest<IndexLevelRequest> {

    private String repository;
    private String index;

    public IndexLevelRequest() {
    }

    public IndexLevelRequest(String repository, String index) {
        this.repository = repository;
        this.index = index;
    }

    public String repository() {
        return repository;
    }

    public String index() {
        return index;
    }

    public String getTaskId() {
        return taskId(repository, index);
    }

    @Override
    public ActionRequestValidationException validate() {
        return Actions.notNull("repository", repository, "index", index);
    }

    public static String taskId(String repository, String index) {
        return repository + ":" + index + "-task";
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        repository = in.readString();
        index = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(repository);
        out.writeString(index);
    }

    /**
     * RequestBuilder
     */
    public static class Builder extends MasterNodeOperationRequestBuilder<IndexLevelRequest, AcknowledgedResponse, Builder> {

        protected Builder(ElasticsearchClient client, Action<IndexLevelRequest, AcknowledgedResponse, Builder> action, IndexLevelRequest request) {
            super(client, action, request);
        }
    }



}
