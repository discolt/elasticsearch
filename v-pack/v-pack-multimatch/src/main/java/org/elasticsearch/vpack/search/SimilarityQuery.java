package org.elasticsearch.vpack.search;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;

import java.io.IOException;
import java.util.Objects;

public class SimilarityQuery extends Query {

    private final Query query;
    private final Similarity similarity;

    public SimilarityQuery(Query query, Similarity similarity) {
        this.query = Objects.requireNonNull(query);
        this.similarity = similarity;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
        searcher.setSimilarity(similarity);
        return query.createWeight(searcher, needsScores, boost);
    }

    @Override
    public String toString(String field) {
        return query.toString();
    }

    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) &&
                equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(SimilarityQuery other) {
        return query.equals(other.query) &&
                similarity.equals(other.similarity);
    }

    @Override
    public int hashCode() {
        int h = classHash();
        h = 31 * h + query.hashCode();
        h = 31 * h + similarity.hashCode();
        return h;
    }
}
