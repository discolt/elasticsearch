package org.elasticsearch.search.join;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.join.terms.TermsFetchRequest;
import org.elasticsearch.search.join.terms.TermsFetchResponse;
import org.elasticsearch.search.join.terms.TransportTermsFetchAction;
import org.elasticsearch.search.join.terms.collector.TermsReader;
import org.elasticsearch.search.profile.SearchProfileShardResults;

import java.util.List;

public class JoinQueryRewriter {


    /**
     * Rewrite SearchRequest QuerySource
     */
    public static boolean rewrite(TransportTermsFetchAction transportTermsFetchAction, SearchRequest searchRequest) {
        boolean isPruned = false;
        if (searchRequest.source().joins() != null && !searchRequest.source().joins().isEmpty()) {
            SearchSourceBuilder origSource = searchRequest.source();
            List<JoinBuilder> joins = origSource.joins();
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            for (JoinBuilder join : joins) {
                TermsFetchRequest request = new TermsFetchRequest();
                request.indices(join.indices()).field(join.field()).source(buildSource(join.query()));
                TermsFetchResponse resp = transportTermsFetchAction.execute(request).actionGet();
                if (resp.getShardFailures().length > 0) {
                    throw new RuntimeException(resp.getShardFailures()[0].getCause());
                }
                isPruned = resp.isPruned();
                List terms = TermsReader.readList(resp.getEncodedTermsSet());
                TermsQueryBuilder termsQueryBuilder = new TermsQueryBuilder(join.sourceField(), terms);
                boolQueryBuilder.filter(termsQueryBuilder);
//                boolQueryBuilder.must(termsQueryBuilder);
            }
            if (origSource.query() != null) {
                boolQueryBuilder.filter(origSource.query());
            }
            searchRequest.source(buildSource(boolQueryBuilder));
        }
        return isPruned;
    }

    private static SearchSourceBuilder buildSource(QueryBuilder queryBuilder) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);
        return searchSourceBuilder;
    }

    public static ActionListener<SearchResponse> rewriteListener(ActionListener<SearchResponse> listener, boolean isPrund) {
        return ActionListener.wrap(
                response -> {
                    if (isPrund) {
                        // set terminatedEarly true
                        SearchResponseSections internalResponse = response.internalResponse();
                        SearchResponseSections wrappedInternalResponse =
                                new SearchResponseSections(internalResponse.hits(), internalResponse.aggregations(), internalResponse.suggest(), internalResponse.timedOut(), true,
                                        new SearchProfileShardResults(internalResponse.profile()), internalResponse.getNumReducePhases());
                        SearchResponse wrappedSearchResponse = new SearchResponse(wrappedInternalResponse, response.getScrollId(), response.getTotalShards(), response.getSuccessfulShards(),
                                response.getSkippedShards(), response.getTook().millis(), response.getShardFailures(), response.getClusters());
                        listener.onResponse(wrappedSearchResponse);
                    } else {
                        listener.onResponse(response);
                    }
                },
                error -> {
                    listener.onFailure(error);
                }
        );
    }

}
