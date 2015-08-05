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

package org.apache.lucene.search;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.GeoUtils;
import org.apache.lucene.util.SparseFixedBitSet;

import java.io.IOException;

/**
 * Custom ConstantScoreWrapper for {@code GeoPointTermQuery} that cuts over to DocValues
 * for post filtering boundary ranges.
 *
 * @lucene.experimental
 */
final class GeoPointTermQueryConstantScoreWrapper <Q extends GeoPointTermQuery> extends Query {
  protected final Q query;

  protected GeoPointTermQueryConstantScoreWrapper(Q query) {
    this.query = query;
  }

  @Override
  public String toString(String field) {
    return query.toString();
  }

  @Override
  public final boolean equals(final Object o) {
    if (super.equals(o) == false) {
      return false;
    }
    final GeoPointTermQueryConstantScoreWrapper<?> that = (GeoPointTermQueryConstantScoreWrapper<?>) o;
    return this.query.equals(that.query) && this.getBoost() == that.getBoost();
  }

  @Override
  public final int hashCode() {
    return 31 * super.hashCode() + query.hashCode();
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return new ConstantScoreWeight(this) {

      private DocIdSet getDocIDs(LeafReaderContext context, Bits acceptDocs) throws IOException {
        final Terms terms = context.reader().terms(query.field);
        if (terms == null) {
          return DocIdSet.EMPTY;
        }

        final GeoPointTermsEnum termsEnum = (GeoPointTermsEnum)(query.getTermsEnum(terms));
        assert termsEnum != null;

        LeafReader reader = context.reader();
        BitDocIdSet.Builder builder = new BitDocIdSet.Builder(reader.maxDoc());
        PostingsEnum docs = null;
        SortedNumericDocValues sdv = reader.getSortedNumericDocValues(query.field);

        while (termsEnum.next() != null) {
          docs = termsEnum.postings(acceptDocs, docs, PostingsEnum.NONE);
          // boundary terms need post filtering by
          if (termsEnum.boundaryTerm()) {
            int docId = docs.nextDoc();
            BitDocIdSet bis = new BitDocIdSet(new SparseFixedBitSet(reader.maxDoc()));
            do {
              sdv.setDocument(docId);
              for (int i=0; i<sdv.count(); ++i) {
                final long hash = sdv.valueAt(i);
                final double lon = GeoUtils.mortonUnhashLon(hash);
                final double lat = GeoUtils.mortonUnhashLat(hash);
                if (termsEnum.postFilter(lon, lat)) {
                  bis.bits().set(docId);
                }
              }
            } while ((docId = docs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS);
            builder.or(bis.iterator());
          } else {
            builder.or(docs);
          }
        }

        return builder.build();
      }

      private Scorer scorer(DocIdSet set) throws IOException {
        if (set == null) {
          return null;
        }
        final DocIdSetIterator disi = set.iterator();
        if (disi == null) {
          return null;
        }
        return new ConstantScoreScorer(this, score(), disi);
      }

      @Override
      public BulkScorer bulkScorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
        final Scorer scorer = scorer(getDocIDs(context, acceptDocs));
        if (scorer == null) {
          return null;
        }
        return new DefaultBulkScorer(scorer);
      }

      @Override
      public Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
        return scorer(getDocIDs(context, acceptDocs));
      }
    };
  }
}
