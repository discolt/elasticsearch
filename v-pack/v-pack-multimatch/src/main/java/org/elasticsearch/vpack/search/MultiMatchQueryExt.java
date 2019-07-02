/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.vpack.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.BlendedTermQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.index.similarity.SimilarityService;

import java.io.IOException;
import java.util.*;

public class MultiMatchQueryExt extends MatchQuery {

    private Float groupTieBreaker = null;
    private SimilarityService similarityService;

    public void setTieBreaker(float tieBreaker) {
        this.groupTieBreaker = tieBreaker;
    }

    public MultiMatchQueryExt(QueryShardContext context) {
        super(context);
        similarityService = context.getSimilarityService();
    }

    private Query parseAndApply(Type type, String fieldName, Object value, String minimumShouldMatch, Float boostValue, String similarity) throws IOException {
        Query query = parse(type, fieldName, value);
        query = Queries.maybeApplyMinimumShouldMatch(query, minimumShouldMatch);
        if (query != null && boostValue != null && boostValue != AbstractQueryBuilder.DEFAULT_BOOST && query instanceof MatchNoDocsQuery == false) {
            query = new BoostQuery(query, boostValue);
        }
        if (query != null && similarity != null) {
            Similarity fieldSim = similarityService.getSimilarity(similarity).get();
            query = new SimilarityQuery(query, fieldSim);
        }
        return query;
    }

    public Query parse(MultiMatchQueryExtBuilder.Type type, Map<String, Map.Entry<Float, String>> fieldNames, Object value, String minimumShouldMatch) throws IOException {
        Query result;
        if (fieldNames.size() == 1) {
            Map.Entry<String, Map.Entry<Float, String>> fieldBoost = fieldNames.entrySet().iterator().next();
            Float boostValue = fieldBoost.getValue().getKey();
            String similarity = fieldBoost.getValue().getValue();
            result = parseAndApply(type.matchQueryType(), fieldBoost.getKey(), value, minimumShouldMatch, boostValue, similarity);
        } else {
            final float tieBreaker = groupTieBreaker == null ? type.tieBreaker() : groupTieBreaker;
            switch (type) {
                case PHRASE:
                case PHRASE_PREFIX:
                case BEST_FIELDS:
                case MOST_FIELDS:
                    queryBuilder = new QueryBuilder(tieBreaker);
                    break;
                case CROSS_FIELDS:
                    queryBuilder = new CrossFieldsQueryBuilder(tieBreaker);
                    break;
                default:
                    throw new IllegalStateException("No such type: " + type);
            }
            final List<? extends Query> queries = queryBuilder.buildGroupedQueries(type, fieldNames, value, minimumShouldMatch);
            result = queryBuilder.combineGrouped(queries);
        }
        return result;
    }

    private QueryBuilder queryBuilder;

    public class QueryBuilder {
        protected final float tieBreaker;

        public QueryBuilder(float tieBreaker) {
            this.tieBreaker = tieBreaker;
        }

        public List<Query> buildGroupedQueries(MultiMatchQueryExtBuilder.Type type, Map<String, Map.Entry<Float, String>> fieldNames, Object value, String minimumShouldMatch) throws IOException {
            List<Query> queries = new ArrayList<>();
            for (String fieldName : fieldNames.keySet()) {
                Float boostValue = fieldNames.get(fieldName).getKey();
                String simValue = fieldNames.get(fieldName).getValue();
                Query query = parseGroup(type.matchQueryType(), fieldName, boostValue, simValue, value, minimumShouldMatch);
                if (query != null) {
                    queries.add(query);
                }
            }
            return queries;
        }

        public Query parseGroup(Type type, String field, Float boostValue, String simValue, Object value, String minimumShouldMatch) throws IOException {
            return parseAndApply(type, field, value, minimumShouldMatch, boostValue, simValue);
        }

        private Query combineGrouped(List<? extends Query> groupQuery) {
            if (groupQuery == null || groupQuery.isEmpty()) {
                return new MatchNoDocsQuery("[multi_match] list of group queries was empty");
            }
            if (groupQuery.size() == 1) {
                return groupQuery.get(0);
            }
            List<Query> queries = new ArrayList<>();
            for (Query query : groupQuery) {
                queries.add(query);
            }
            return new DisjunctionMaxQuery(queries, tieBreaker);
        }

        public Query blendTerm(Term term, MappedFieldType fieldType) {
            return MultiMatchQueryExt.super.blendTermQuery(term, fieldType);
        }

        public Query blendTerms(Term[] terms, MappedFieldType fieldType) {
            return MultiMatchQueryExt.super.blendTermsQuery(terms, fieldType);
        }

        public Query termQuery(MappedFieldType fieldType, BytesRef value) {
            return MultiMatchQueryExt.this.termQuery(fieldType, value, lenient);
        }

        public Query blendPhrase(PhraseQuery query, MappedFieldType type) {
            return MultiMatchQueryExt.super.blendPhraseQuery(query, type);
        }
    }

    final class CrossFieldsQueryBuilder extends QueryBuilder {
        private FieldAndFieldType[] blendedFields;

        CrossFieldsQueryBuilder(float tiebreaker) {
            super(tiebreaker);
        }

        @Override
        public List<Query> buildGroupedQueries(MultiMatchQueryExtBuilder.Type type, Map<String, Map.Entry<Float, String>> fieldNames, Object value, String minimumShouldMatch) throws IOException {
            Map<Analyzer, List<FieldAndFieldType>> groups = new HashMap<>();
            List<Query> queries = new ArrayList<>();
            for (Map.Entry<String, Map.Entry<Float, String>> entry : fieldNames.entrySet()) {
                String name = entry.getKey();
                MappedFieldType fieldType = context.fieldMapper(name);
                if (fieldType != null) {
                    Analyzer actualAnalyzer = getAnalyzer(fieldType, type == MultiMatchQueryExtBuilder.Type.PHRASE);
                    name = fieldType.name();
                    if (!groups.containsKey(actualAnalyzer)) {
                        groups.put(actualAnalyzer, new ArrayList<>());
                    }
                    Float boost = entry.getValue().getKey();
                    boost = boost == null ? Float.valueOf(1.0f) : boost;
                    String similarity = entry.getValue().getValue();
                    groups.get(actualAnalyzer).add(new FieldAndFieldType(fieldType, boost, similarity));
                } else {
                    queries.add(new MatchNoDocsQuery("unknown field " + name));
                }
            }
            for (List<FieldAndFieldType> group : groups.values()) {
                if (group.size() > 1) {
                    blendedFields = new FieldAndFieldType[group.size()];
                    int i = 0;
                    for (FieldAndFieldType fieldAndFieldType : group) {
                        blendedFields[i++] = fieldAndFieldType;
                    }
                } else {
                    blendedFields = null;
                }
                /*
                 * We have to pick some field to pass through the superclass so
                 * we just pick the first field. It shouldn't matter because
                 * fields are already grouped by their analyzers/types.
                 */
                String representativeField = group.get(0).fieldType.name();
                String representativeSimilarity = group.get(0).similarity;
                Query q = parseGroup(type.matchQueryType(), representativeField, 1f, representativeSimilarity, value, minimumShouldMatch); // TODO sim is null
                if (q != null) {
                    queries.add(q);
                }
            }

            return queries.isEmpty() ? null : queries;
        }

        @Override
        public Query blendTerms(Term[] terms, MappedFieldType fieldType) {
            if (blendedFields == null || blendedFields.length == 1) {
                return super.blendTerms(terms, fieldType);
            }
            BytesRef[] values = new BytesRef[terms.length];
            for (int i = 0; i < terms.length; i++) {
                values[i] = terms[i].bytes();
            }
            return MultiMatchQueryExt.blendTerms(context, values, commonTermsCutoff, tieBreaker, blendedFields);
        }

        @Override
        public Query blendTerm(Term term, MappedFieldType fieldType) {
            if (blendedFields == null) {
                return super.blendTerm(term, fieldType);
            }
            return MultiMatchQueryExt.blendTerm(context, term.bytes(), commonTermsCutoff, tieBreaker, blendedFields);
        }

        @Override
        public Query termQuery(MappedFieldType fieldType, BytesRef value) {
            /*
             * Use the string value of the term because we're reusing the
             * portion of the query is usually after the analyzer has run on
             * each term. We just skip that analyzer phase.
             */
            return blendTerm(new Term(fieldType.name(), value.utf8ToString()), fieldType);
        }

        @Override
        public Query blendPhrase(PhraseQuery query, MappedFieldType type) {
            if (blendedFields == null) {
                return super.blendPhrase(query, type);
            }
            /**
             * We build phrase queries for multi-word synonyms when {@link QueryBuilder#autoGenerateSynonymsPhraseQuery} is true.
             */
            return MultiMatchQueryExt.blendPhrase(query, tieBreaker, blendedFields);
        }
    }

    static Query blendTerm(QueryShardContext context, BytesRef value, Float commonTermsCutoff, float tieBreaker,
                           FieldAndFieldType... blendedFields) {
        return blendTerms(context, new BytesRef[]{value}, commonTermsCutoff, tieBreaker, blendedFields);
    }

    static Query blendTerms(QueryShardContext context, BytesRef[] values, Float commonTermsCutoff, float tieBreaker,
                            FieldAndFieldType... blendedFields) {
        List<Query> queries = new ArrayList<>();
        Term[] terms = new Term[blendedFields.length * values.length];
        float[] blendedBoost = new float[blendedFields.length * values.length];
        int i = 0;
        for (FieldAndFieldType ft : blendedFields) {
            for (BytesRef term : values) {
                Query query;
                try {
                    query = ft.fieldType.termQuery(term, context);
                } catch (IllegalArgumentException e) {
                    // the query expects a certain class of values such as numbers
                    // of ip addresses and the value can't be parsed, so ignore this
                    // field
                    continue;
                } catch (ElasticsearchParseException parseException) {
                    // date fields throw an ElasticsearchParseException with the
                    // underlying IAE as the cause, ignore this field if that is
                    // the case
                    if (parseException.getCause() instanceof IllegalArgumentException) {
                        continue;
                    }
                    throw parseException;
                }
                float boost = ft.boost;
                while (query instanceof BoostQuery) {
                    BoostQuery bq = (BoostQuery) query;
                    query = bq.getQuery();
                    boost *= bq.getBoost();
                }
                if (query.getClass() == TermQuery.class) {
                    terms[i] = ((TermQuery) query).getTerm();
                    blendedBoost[i] = boost;
                    i++;
                } else {
                    if (boost != 1f && query instanceof MatchNoDocsQuery == false) {
                        query = new BoostQuery(query, boost);
                    }
                    queries.add(query);
                }
            }
        }
        if (i > 0) {
            terms = Arrays.copyOf(terms, i);
            blendedBoost = Arrays.copyOf(blendedBoost, i);
            if (commonTermsCutoff != null) {
                queries.add(BlendedTermQuery.commonTermsBlendedQuery(terms, blendedBoost, commonTermsCutoff));
            } else {
                queries.add(BlendedTermQuery.dismaxBlendedQuery(terms, blendedBoost, tieBreaker));
            }
        }
        if (queries.size() == 1) {
            return queries.get(0);
        } else {
            // best effort: add clauses that are not term queries so that they have an opportunity to match
            // however their score contribution will be different
            // TODO: can we improve this?
            return new DisjunctionMaxQuery(queries, 1.0f);
        }
    }

    /**
     * Expand a {@link PhraseQuery} to multiple fields that share the same analyzer.
     * Returns a {@link DisjunctionMaxQuery} with a disjunction for each expanded field.
     */
    static Query blendPhrase(PhraseQuery query, float tiebreaker, FieldAndFieldType... fields) {
        List<Query> disjunctions = new ArrayList<>();
        for (FieldAndFieldType field : fields) {
            int[] positions = query.getPositions();
            Term[] terms = query.getTerms();
            PhraseQuery.Builder builder = new PhraseQuery.Builder();
            for (int i = 0; i < terms.length; i++) {
                builder.add(new Term(field.fieldType.name(), terms[i].bytes()), positions[i]);
            }
            Query q = builder.build();
            if (field.boost != AbstractQueryBuilder.DEFAULT_BOOST) {
                q = new BoostQuery(q, field.boost);
            }
            disjunctions.add(q);
        }
        return new DisjunctionMaxQuery(disjunctions, tiebreaker);
    }

    @Override
    protected Query blendTermQuery(Term term, MappedFieldType fieldType) {
        if (queryBuilder == null) {
            return super.blendTermQuery(term, fieldType);
        }
        return queryBuilder.blendTerm(term, fieldType);
    }

    @Override
    protected Query blendTermsQuery(Term[] terms, MappedFieldType fieldType) {
        if (queryBuilder == null) {
            return super.blendTermsQuery(terms, fieldType);
        }
        return queryBuilder.blendTerms(terms, fieldType);
    }

    @Override
    protected Query blendPhraseQuery(PhraseQuery query, MappedFieldType fieldType) {
        if (queryBuilder == null) {
            return super.blendPhraseQuery(query, fieldType);
        }
        return queryBuilder.blendPhrase(query, fieldType);
    }

    static final class FieldAndFieldType {
        final MappedFieldType fieldType;
        final float boost;
        final String similarity;

        FieldAndFieldType(MappedFieldType fieldType, float boost, String similarity) {
            this.fieldType = Objects.requireNonNull(fieldType);
            this.boost = boost;
            this.similarity = similarity;
        }
    }
}
