package org.elasticsearch.vpack.xdcr.cluster;

import org.elasticsearch.ElasticsearchException;

public class IndexXdcrCreationException extends ElasticsearchException {

    public IndexXdcrCreationException(String msg) {
        super(msg);
    }
}
