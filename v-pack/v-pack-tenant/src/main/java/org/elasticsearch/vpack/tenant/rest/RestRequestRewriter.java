package org.elasticsearch.vpack.tenant.rest;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public interface RestRequestRewriter {

    default Optional<ParamsRewriter> paramsRewriter() {
        return Optional.empty();
    }

    default Optional<ContentRewriter> contentRewriter() {
        return Optional.empty();
    }

    interface ParamsRewriter {
        void modify(Map<String, String> params, String tenant);
    }

    interface ContentRewriter {
        String content(String content, String tenant) throws IOException;
    }
}
