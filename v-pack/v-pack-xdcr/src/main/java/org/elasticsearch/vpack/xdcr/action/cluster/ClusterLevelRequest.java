package org.elasticsearch.vpack.xdcr.action.cluster;

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

public class ClusterLevelRequest extends AcknowledgedRequest<ClusterLevelRequest> {

    private String repository;

    private String excludes;

    public ClusterLevelRequest() {
    }

    public ClusterLevelRequest(String repository) {
        this.repository = repository;
        this.excludes = "";
    }

    public ClusterLevelRequest(String repository, String excludes) {
        this.repository = repository;
        this.excludes = excludes;
    }

    public String repository() {
        return repository;
    }

    public String exclusions() {
        return excludes;
    }


    public String getTaskId() {
        return repository + "-task";
    }

    @Override
    public ActionRequestValidationException validate() {
        return Actions.notNull("repository", repository);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        repository = in.readString();
        excludes = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(repository);
        out.writeString(excludes);
    }


    /**
     * RequestBuilder
     */
    public static class Builder extends MasterNodeOperationRequestBuilder<ClusterLevelRequest, AcknowledgedResponse, Builder> {

        protected Builder(ElasticsearchClient client, Action<ClusterLevelRequest, AcknowledgedResponse, Builder> action, ClusterLevelRequest request) {
            super(client, action, request);
        }
    }

}
