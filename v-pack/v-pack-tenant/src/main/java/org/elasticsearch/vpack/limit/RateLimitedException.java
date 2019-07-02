package org.elasticsearch.vpack.limit;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;

public class RateLimitedException extends ElasticsearchStatusException {

    public RateLimitedException(String msg, Object... args) {
        super(msg, RestStatus.TOO_MANY_REQUESTS, args);
    }

}
