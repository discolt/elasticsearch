package org.elasticsearch.vpack.xdcr.cluster;///*
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Repository missing exception
 */
public class RepositoryMissingException extends RepositoryException {


    public RepositoryMissingException(String repository) {
        super(repository, "missing");
    }

    @Override
    public RestStatus status() {
        return RestStatus.NOT_FOUND;
    }

    public RepositoryMissingException(StreamInput in) throws IOException{
        super(in);
    }
}
