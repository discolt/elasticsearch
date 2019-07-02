/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.vpack.tenant.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.ByteBufferReference;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.*;
import org.elasticsearch.vpack.TenantContext;
import org.elasticsearch.vpack.tenant.IndexTenantRewriter;
import org.elasticsearch.vpack.tenant.PathTenantRewriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * 替换URL(PathTire)里的index
 * 替换部分Request Body里的index，若Action级别的index不好替换。
 * 替换返回内容
 */
public class TenantRestHandler implements RestHandler {

    private static Logger logger = LogManager.getLogger(TenantRestHandler.class);

    private static final String CONTENT_TYPE = "Content-Type";
    private final ThreadContext threadContext;
    private final RestHandler restHandler;

    private RestRewriterDispacher requestDispacher = new RestRewriterDispacher();

    public TenantRestHandler(ThreadContext threadContext, RestHandler restHandler) {
        this.threadContext = threadContext;
        this.restHandler = restHandler;
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        String tenant = TenantContext.tenant(threadContext);
        if (tenant != null) {
            RestRequest rewritedRequest = rewriteRequest(request, tenant);
            boolean notFilterResp = rewritedRequest.paramAsBoolean("o", false);
            if (!notFilterResp) {
                channel = rewriteChannel(channel, tenant);
            }
            restHandler.handleRequest(rewritedRequest, channel, client);
        } else {
            restHandler.handleRequest(request, channel, client);
        }
    }

    // ** ---------------------------------------------
    // ** -----------  RestRequest Rewrite  ----------
    // ** ---------------------------------------------

    private RestRequest rewriteRequest(RestRequest request, String tenant) {

        // uri改写index(PathTire)
        Map<String, String> params = request.params();
        String path = request.path();
        if (params.containsKey("index")) {
            String index = params.get("index");
            params.put("index", IndexTenantRewriter.rewrite(index, tenant));
        }
        // uri内改写pipline(PathTire)
        if (params.containsKey("pipeline")) {
            String pipeline = params.get("pipeline");
            params.put("pipeline", PathTenantRewriter.rewrite(pipeline, tenant));
        }

        // Header 处理
        Map<String, List<String>> headers = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> entry : request.getHeaders().entrySet()) {
            if (entry.getKey().equalsIgnoreCase(CONTENT_TYPE)) {
                List jsonContentType = CollectionUtils.asArrayList("application/json");
                headers.put(CONTENT_TYPE, jsonContentType);
            } else {
                headers.put(entry.getKey(), entry.getValue());
            }
        }
        headers = Collections.unmodifiableMap(headers);

        // params处理
        Optional<RestRequestRewriter> requestRewriterOptional = requestDispacher.getHandler(request);
        if (requestRewriterOptional.isPresent() && requestRewriterOptional.get().paramsRewriter().isPresent()) {
            requestRewriterOptional.get().paramsRewriter().get().modify(params, tenant);
        }

        // Create new RestRequest with content rewriter
        return new RestRequest(request.getXContentRegistry(), params, path, headers) {
            {
                //consumedParams
                param("pretty");
                param("format");
                param("error_trace");
                param("human");
                param("filter_path");
            }

            @Override
            public Method method() {
                return request.method();
            }

            @Override
            public String uri() {
                return request.uri();
            }

            @Override
            public boolean hasContent() {
                return request.hasContent();
            }

            @Override
            public BytesReference content() {
                if (requestRewriterOptional.isPresent() && requestRewriterOptional.get().contentRewriter().isPresent()) {
                    try {
                        RestRequestRewriter.ContentRewriter contentRewriter = requestRewriterOptional.get().contentRewriter().get();
                        String content = contentRewriter.content(request.content().utf8ToString(), tenant);
                        ByteBuffer byteBuffer = ByteBuffer.wrap(content.getBytes());
                        return new ByteBufferReference(byteBuffer);
                    } catch (Exception e) {
                        throw new ElasticsearchException(e);
                    }
                } else {
                    return request.content();
                }
            }
        };
    }

    // ** ---------------------------------------------
    // ** -----------  RestResponse Rewrite  ----------
    // ** ---------------------------------------------

    private RestChannel rewriteChannel(RestChannel channel, String tenant) {

        return new RestChannel() {
            @Override
            public XContentBuilder newBuilder() throws IOException {
                return channel.newBuilder();
            }

            @Override
            public XContentBuilder newErrorBuilder() throws IOException {
                return channel.newErrorBuilder();
            }

            @Override
            public XContentBuilder newBuilder(XContentType xContentType, boolean useFiltering) throws IOException {
                return channel.newBuilder(xContentType, useFiltering);
            }

            @Override
            public BytesStreamOutput bytesOutput() {
                return channel.bytesOutput();
            }

            @Override
            public RestRequest request() {
                return channel.request();
            }

            @Override
            public boolean detailedErrorsEnabled() {
                return channel.detailedErrorsEnabled();
            }

            @Override
            public void sendResponse(RestResponse response) {
                String content = response.content().utf8ToString();
                content = ((RestResponseRewriter) (c, t) -> c.replaceAll(t + "\\.", "")).rewrite(content, tenant);
                RestResponse replacedResponse = new BytesRestResponse(response.status(), response.contentType(), content);
                channel.sendResponse(replacedResponse);
            }
        };
    }

    public interface RestResponseRewriter {
        String rewrite(String content, String tenant);
    }

}
