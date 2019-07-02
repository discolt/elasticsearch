package org.elasticsearch.vpack.tenant.action;

import org.elasticsearch.action.ActionRequest;

public interface ActionRequestRewriter<Request extends ActionRequest> {

    Request rewrite(Request request, String tenant);

}