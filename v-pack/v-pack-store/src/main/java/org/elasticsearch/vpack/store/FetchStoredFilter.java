package org.elasticsearch.vpack.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.ByteBufferReference;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.tasks.Task;

import java.nio.ByteBuffer;
import java.util.*;

import static org.elasticsearch.action.ActionListener.wrap;

public class FetchStoredFilter implements ActionFilter {

    private static final Logger logger = LogManager.getLogger(FetchStoredFilter.class);

    private final Client client;
    private final DetachStoredIndices detachStored;

    public FetchStoredFilter(Client client, DetachStoredIndices detachStored) {
        this.client = client;
        this.detachStored = detachStored;
    }

    @Override
    public int order() {
        return 10000;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
            Task task, String action, Request request,
            ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {

        if (detachStored.isEmpty()) {
            chain.proceed(task, action, request, listener);
            return;
        }

        // 处理search结果
        if (SearchAction.NAME.equals(action) || SearchScrollAction.NAME.equals(action)) {
            ActionListener<SearchResponse> wrapped = wrap(searchResponse -> listener.onResponse((Response) fetchSearchResponse(searchResponse)), listener::onFailure);
            chain.proceed(task, action, request, (ActionListener<Response>) wrapped);
        } else if (GetAction.NAME.equals(action)) {
            ActionListener<GetResponse> wrapped = wrap(getResponse -> listener.onResponse((Response) fetchGetResponse(getResponse)), listener::onFailure);
            chain.proceed(task, action, request, (ActionListener<Response>) wrapped);
        } else if (MultiGetAction.NAME.equals(action)) {
            ActionListener<MultiGetResponse> wrapped = wrap(multiGetResponse -> listener.onResponse((Response) fetchMultiGetResponse(multiGetResponse)), listener::onFailure);
            chain.proceed(task, action, request, (ActionListener<Response>) wrapped);
        } else {
            chain.proceed(task, action, request, listener);
        }
    }

    /**
     * 处理Search
     *
     * @param searchResponse
     * @return
     */
    private SearchResponse fetchSearchResponse(SearchResponse searchResponse) {
        final long totalHits = searchResponse.getHits().getTotalHits();
        if (totalHits == 0) {
            return searchResponse;
        }
        // 添加待抓取source的entry(index,id)
        Map<String, List<Entry>> wantFetch = new HashMap<>();
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            if (hit.hasSource()) {
                continue;
            }
            String index = hit.getIndex();
            String store = detachStored.getStore(index);
            if (Strings.isNullOrEmpty(store)) {
                continue;
            } else {
                wantFetch.computeIfAbsent(store, key -> new ArrayList<>());
                wantFetch.get(store).add(new Entry(hit.getIndex(), hit.getType(), hit.getId()));
            }
        }
        if (wantFetch.isEmpty()) {
            return searchResponse;
        }
        // 抓取远程source结果组装sourceMap
        Map<String, BytesReference> sourceMap = new HashMap<>();
        wantFetch.forEach((store, entries) -> sourceMap.putAll(fetch(client, store, entries)));
        searchResponse.getHits().forEach(
                hit -> {
                    if (!hit.hasSource()) {
                        hit.sourceRef(sourceMap.get(key(hit.getIndex(), hit.getId())));
                    }
                }
        );
        // 重组搜索结果
        SearchHits newHits = new SearchHits(hits, searchResponse.getHits().totalHits, searchResponse.getHits().getMaxScore());
        SearchResponseSections internalResponse = new SearchResponseSections(
                newHits,
                searchResponse.getAggregations(), searchResponse.getSuggest(),
                searchResponse.isTimedOut(), searchResponse.isTerminatedEarly(),
                new SearchProfileShardResults(searchResponse.getProfileResults()), searchResponse.getNumReducePhases());
        SearchResponse newResp = new SearchResponse(internalResponse,
                searchResponse.getScrollId(), searchResponse.getTotalShards(), searchResponse.getSuccessfulShards(), searchResponse.getSkippedShards(),
                searchResponse.getTook().millis(), searchResponse.getShardFailures(), searchResponse.getClusters());
        return newResp;
    }


    /**
     * 处理Get
     *
     * @param response
     * @return
     */
    private GetResponse fetchGetResponse(GetResponse response) {
        if (!response.isExists() || !response.isSourceEmpty()) {
            return response;
        }
        String index = response.getIndex();
        String store = detachStored.getStore(index);
        if (Strings.isEmpty(store)) {
            return response;
        }
        List<Entry> entries = Arrays.asList(new Entry(response.getIndex(), response.getType(), response.getId()));
        Map<String, BytesReference> sourceMap = fetch(client, store, entries);
        BytesReference source = sourceMap.isEmpty() ? null : sourceMap.entrySet().iterator().next().getValue();
        GetResult getResult = new GetResult(response.getIndex(), response.getType(), response.getId(), response.getSeqNo(),
                response.getPrimaryTerm(), response.getVersion(), response.isExists(),
                source,
                response.getFields());
        return new GetResponse(getResult);
    }

    /**
     * 处理MulitGet
     *
     * @param response
     * @return
     */
    private MultiGetResponse fetchMultiGetResponse(MultiGetResponse response) {
        MultiGetItemResponse[] itemResponses = response.getResponses();
        if (itemResponses.length == 0) {
            return response;
        }
        MultiGetItemResponse[] newItemResps = new MultiGetItemResponse[itemResponses.length];
        for (int i = 0; i < itemResponses.length; i++) {
            newItemResps[i] = new MultiGetItemResponse(fetchGetResponse(itemResponses[i].getResponse()), itemResponses[i].getFailure());
        }
        return new MultiGetResponse(newItemResps);
    }

    /**
     * 从store集群获取source
     *
     * @param client
     * @param store
     * @param entries
     * @return
     */
    private Map<String, BytesReference> fetch(Client client, String store, List<Entry> entries) {
        Map<String, BytesReference> sourceMap = new HashMap();
        try {
            MultiGetRequestBuilder mget = client.getRemoteClusterClient(store).prepareMultiGet();
            entries.forEach(e -> mget.add(e.index, "_doc", e.id));
            MultiGetResponse mgetResp = mget.execute().get();
            mgetResp.forEach(e -> {
                if (e.getResponse().isExists()) {
                    String source = (String) e.getResponse().getSource().get("source");
                    sourceMap.put(key(e.getIndex(), e.getId()), new ByteBufferReference(ByteBuffer.wrap(source.getBytes())));
                }
            });
        } catch (Exception e) {
            logger.error("fetch remote source error found : {}", e);
        }
        return sourceMap;
    }

    private String key(String index, String id) {
        return index + ":" + id;
    }

    private static class Entry {
        final String index;
        final String type;
        final String id;

        public Entry(String index, String type, String id) {
            this.index = index;
            this.type = type;
            this.id = id;
        }
    }

}
