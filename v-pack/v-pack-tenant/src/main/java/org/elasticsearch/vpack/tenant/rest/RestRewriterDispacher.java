package org.elasticsearch.vpack.tenant.rest;

import org.elasticsearch.vpack.RestHandlerDispacher;
import org.elasticsearch.vpack.tenant.rest.request.*;

import static org.elasticsearch.rest.RestRequest.Method.*;

public class RestRewriterDispacher extends RestHandlerDispacher<RestRequestRewriter> {

    @Override
    protected void register() {
        RestSearchRequestRewriter searchRequestRewriter = new RestSearchRequestRewriter();
        this.registerHandler(GET, "/_search", searchRequestRewriter);
        this.registerHandler(POST, "/_search", searchRequestRewriter);
        this.registerHandler(GET, "/{index}/_search", searchRequestRewriter);
        this.registerHandler(POST, "/{index}/_search", searchRequestRewriter);
        this.registerHandler(GET, "/{index}/{type}/_search", searchRequestRewriter);
        this.registerHandler(POST, "/{index}/{type}/_search", searchRequestRewriter);

        RestMultiSearchRequestRewriter multiSearchRequestRewriter = new RestMultiSearchRequestRewriter();
        this.registerHandler(GET, "/_msearch", multiSearchRequestRewriter);
        this.registerHandler(POST, "/_msearch", multiSearchRequestRewriter);
        this.registerHandler(GET, "/{index}/_msearch", multiSearchRequestRewriter);
        this.registerHandler(POST, "/{index}/_msearch", multiSearchRequestRewriter);
        this.registerHandler(GET, "/{index}/{type}/_msearch", multiSearchRequestRewriter);
        this.registerHandler(POST, "/{index}/{type}/_msearch", multiSearchRequestRewriter);

        RestSearchTemplateRequestRewriter searchTemplateRequestRewriter = new RestSearchTemplateRequestRewriter();
        this.registerHandler(GET, "/_search/template", searchTemplateRequestRewriter);
        this.registerHandler(POST, "/_search/template", searchTemplateRequestRewriter);
        this.registerHandler(GET, "/{index}/_search/template", searchTemplateRequestRewriter);
        this.registerHandler(POST, "/{index}/_search/template", searchTemplateRequestRewriter);
        this.registerHandler(GET, "/{index}/{type}/_search/template", searchTemplateRequestRewriter);
        this.registerHandler(POST, "/{index}/{type}/_search/template", searchTemplateRequestRewriter);
        this.registerHandler(GET, "/_render/template", searchTemplateRequestRewriter);
        this.registerHandler(POST, "/_render/template", searchTemplateRequestRewriter);
        this.registerHandler(GET, "/_render/template/{id}", searchTemplateRequestRewriter);
        this.registerHandler(POST, "/_render/template/{id}", searchTemplateRequestRewriter);

        RestMultiSearchTemplateRequestRewriter multiSearchTemplateRequestRewriter = new RestMultiSearchTemplateRequestRewriter();
        this.registerHandler(GET, "/_msearch/template", multiSearchTemplateRequestRewriter);
        this.registerHandler(POST, "/_msearch/template", multiSearchTemplateRequestRewriter);
        this.registerHandler(GET, "/{index}/_msearch/template", multiSearchTemplateRequestRewriter);
        this.registerHandler(POST, "/{index}/_msearch/template", multiSearchTemplateRequestRewriter);
        this.registerHandler(GET, "/{index}/{type}/_msearch/template", multiSearchTemplateRequestRewriter);
        this.registerHandler(POST, "/{index}/{type}/_msearch/template", multiSearchTemplateRequestRewriter);

        RestPainlessExecuteRequestRewriter painlessExecuteRequestRewriter = new RestPainlessExecuteRequestRewriter();
        this.registerHandler(GET, "/_scripts/painless/_execute", painlessExecuteRequestRewriter);
        this.registerHandler(POST, "/_scripts/painless/_execute", painlessExecuteRequestRewriter);

        RestGetAliasRequestRewriter restGetAliasRequestRewriter = new RestGetAliasRequestRewriter();
        this.registerHandler(GET, "/_alias/{name}", restGetAliasRequestRewriter);
        this.registerHandler(HEAD, "/_alias/{name}", restGetAliasRequestRewriter);
        this.registerHandler(GET, "/{index}/_alias", restGetAliasRequestRewriter);
        this.registerHandler(HEAD, "/{index}/_alias", restGetAliasRequestRewriter);
        this.registerHandler(GET, "/{index}/_alias/{name}", restGetAliasRequestRewriter);
        this.registerHandler(HEAD, "/{index}/_alias/{name}", restGetAliasRequestRewriter);

        RestRankEvalRequestRewriter rankEvalRequestRewriter = new RestRankEvalRequestRewriter();
        this.registerHandler(GET, "/_rank_eval", rankEvalRequestRewriter);
        this.registerHandler(POST, "/_rank_eval", rankEvalRequestRewriter);
        this.registerHandler(GET, "/{index}/_rank_eval", rankEvalRequestRewriter);
        this.registerHandler(POST, "/{index}/_rank_eval", rankEvalRequestRewriter);

        RestGetTemplateRequestRewriter restGetTemplateRequestRewriter = new RestGetTemplateRequestRewriter();
        this.registerHandler(GET, "/_cat/templates", restGetTemplateRequestRewriter);
        this.registerHandler(GET, "/_cat/templates/{name}", restGetTemplateRequestRewriter);

        RestGetIndicesRequestRewriter restGetIndicesRequestRewriter = new RestGetIndicesRequestRewriter();
        this.registerHandler(GET, "/_cat/indices", restGetIndicesRequestRewriter);
        this.registerHandler(GET, "/_cat/indices/{index}", restGetIndicesRequestRewriter);

        RestShardsRequestRewriter shardsReqeustRewriter = new RestShardsRequestRewriter();
        this.registerHandler(GET, "/_cat/shards", shardsReqeustRewriter);
        this.registerHandler(GET, "/_cat/shards/{index}", shardsReqeustRewriter);

        RestSegmentsRequestRewriter segmentsRequestRewriter = new RestSegmentsRequestRewriter();
        this.registerHandler(GET, "/_cat/segments", segmentsRequestRewriter);
        this.registerHandler(GET, "/_cat/segments/{index}", segmentsRequestRewriter);
    }

}
