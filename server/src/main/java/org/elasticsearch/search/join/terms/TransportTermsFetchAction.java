package org.elasticsearch.search.join.terms;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchLocalRequest;
import org.elasticsearch.search.join.JoinSettings;
import org.elasticsearch.search.join.terms.collector.TermsCollector;
import org.elasticsearch.search.join.terms.collector.TermsSet;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class TransportTermsFetchAction extends TransportBroadcastAction<TermsFetchRequest, TermsFetchResponse, TermsFetchShardRequest, TermsFetchShardResponse> {

    private final CircuitBreakerService breakerService;
    private final SearchService searchService;
    private final ThreadPoolExecutor executor;

    @Inject
    public TransportTermsFetchAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                     TransportService transportService, SearchService searchService, ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver,
                                     CircuitBreakerService breakerService) {
        super(settings, TermsFetchAction.NAME, threadPool, clusterService, transportService, actionFilters,
                indexNameExpressionResolver, TermsFetchRequest::new, TermsFetchShardRequest::new, ThreadPool.Names.SEARCH);
        this.searchService = searchService;
        this.breakerService = breakerService;
        int queueSize = JoinSettings.JOIN_THREAD_QUEUE_SIZE.get(settings);
        executor = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>(queueSize));
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * Executes the actions.
     */
    @Override
    protected void doExecute(Task task, TermsFetchRequest request, ActionListener<TermsFetchResponse> listener) {
        request.nowInMillis(System.currentTimeMillis());
        super.doExecute(task, request, listener);
    }


    @Override
    protected TermsFetchShardRequest newShardRequest(int numShards, ShardRouting shard, TermsFetchRequest request) {
        final AliasFilter aliasFilter = searchService.buildAliasFilter(clusterService.state(), shard.getIndexName(), request.indices());
        return new TermsFetchShardRequest(shard.shardId(), aliasFilter, request);
    }

    @Override
    protected TermsFetchShardResponse newShardResponse() {
        return new TermsFetchShardResponse(circuitBreaker());
    }

    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, TermsFetchRequest request, String[] concreteIndices) {
        Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(clusterState, request.routing(), request.indices());
        return clusterService.operationRouting().searchShards(clusterState, concreteIndices, routingMap, request.preference());
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, TermsFetchRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, TermsFetchRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
    }

    private CircuitBreaker circuitBreaker() {
        return breakerService.getBreaker(CircuitBreaker.REQUEST);
    }

    /**
     * Field Terms Query Process
     */
    @Override
    protected TermsFetchShardResponse shardOperation(TermsFetchShardRequest request) throws IOException {
        ShardSearchLocalRequest shardSearchLocalRequest = new ShardSearchLocalRequest(request.shardId(), request.types(),
                request.nowInMillis(), request.filteringAliases());
        shardSearchLocalRequest.source(request.source());
        SearchContext searchContext = SearchContextFactory.createContext(searchService, shardSearchLocalRequest, SearchService.NO_TIMEOUT);
        MappedFieldType fieldType = searchContext.smartNameFieldType(request.field());
        if (fieldType == null) {
            throw new UnsupportedOperationException("[TermsQuery] field '" + request.field() + "' not found for types " + Arrays.toString(request.types()));
        }
        if (!fieldType.hasDocValues()) {
            throw new UnsupportedOperationException("[TermsQuery] field '" + request.field() + "' must enable DocValue");
        }
        searchContext.preProcess(true);
        final ContextIndexSearcher searcher = searchContext.searcher();
        TermsCollector collector = TermsCollector.create(circuitBreaker(), searchContext, fieldType, JoinSettings.JOIN_TERMS_LIMIT.get(settings));
        Future<TermsFetchResult> future = executor.submit(new TermsFetchRunner(searchContext, searcher, collector, searcher::setCheckCancelled));
        try {
            TermsFetchResult result = future.get();
            return new TermsFetchShardResponse(request.shardId(), result.terms(), result.isPruned(), circuitBreaker());
        } catch (Exception e) {
            throw new BroadcastShardOperationFailedException(request.shardId(), e);
        }
    }

    /**
     * Merge Query Result
     */
    @Override
    protected TermsFetchResponse newResponse(TermsFetchRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        int numTerms = 0;
        boolean isPruned = false;
        TermsSet[] termsSets = new TermsSet[shardsResponses.length()];
        List<DefaultShardOperationFailedException> shardFailures = null;

        // we check each shard response
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                // simply ignore non active shards
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = new ArrayList<>();
                }
                logger.error("Shard operation failed", (BroadcastShardOperationFailedException) shardResponse);
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
                // on successful shard response, just add to the array or responses so we can process them below
                // we calculate the total number of terms gathered across each shard so we can use it during
                // initialization of the final TermsResponse below (to avoid rehashing during merging)
                TermsFetchShardResponse shardResp = (TermsFetchShardResponse) shardResponse;
                TermsSet terms = shardResp.getTerms();
                termsSets[i] = terms;
                numTerms += terms.size();
                isPruned = isPruned | shardResp.isPruned();
                successfulShards++;
            }
        }

        if (failedShards > 0) {
            long tookInMillis = System.currentTimeMillis() - request.nowInMillis();
            return new TermsFetchResponse(tookInMillis, shardsResponses.length(), successfulShards, failedShards, shardFailures);
        }

        // Merge the responses
        try {
            // NumericTermsSet is responsible for the merge, set size to avoid rehashing on certain implementations.
            long expectedElements = request.expectedTerms() != null ? request.expectedTerms() : numTerms;
            TermsSet termsSet = TermsSet.newTermsSet(expectedElements, termsSets[0].getEncoding(), circuitBreaker());
            TermsFetchResponse rsp;
            try {
                for (int i = 0; i < termsSets.length; i++) {
                    TermsSet terms = termsSets[i];
                    if (terms != null) {
                        termsSet.merge(terms);
                        terms.release(); // release the shard terms set and adjust the circuit breaker
                        termsSets[i] = null;
                    }
                }

                long tookInMillis = System.currentTimeMillis() - request.nowInMillis();
                rsp = new TermsFetchResponse(termsSet, isPruned, tookInMillis, shardsResponses.length(), successfulShards, failedShards, shardFailures);
            } finally {
                // we can now release the terms set and adjust the circuit breaker, since the TermsByQueryResponse holds an
                // encoded version of the terms set
                termsSet.release();
            }
            return rsp;
        } finally { // If something happens, release the terms sets and adjust the circuit breaker
            for (int i = 0; i < termsSets.length; i++) {
                TermsSet terms = termsSets[i];
                if (terms != null) {
                    terms.release();
                }
            }
        }
    }

}
