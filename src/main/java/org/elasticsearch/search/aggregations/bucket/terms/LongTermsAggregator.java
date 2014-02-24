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
package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.support.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.numeric.NumericValuesSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class LongTermsAggregator extends BucketsAggregator {

    private final InternalOrder order;
    private final int requiredSize;
    private final int shardSize;
    private final long minDocCount;
    private final NumericValuesSource valuesSource;
    private LongHash bucketOrds;
    private LongValues values;

    public LongTermsAggregator(String name, AggregatorFactories factories, NumericValuesSource valuesSource, long estimatedBucketCount,
                               InternalOrder order, int requiredSize, int shardSize, long minDocCount, AggregationContext aggregationContext, Aggregator parent, ExecutionMode executionMode) {
        super(name, BucketAggregationMode.PER_BUCKET, factories, estimatedBucketCount, aggregationContext, parent, executionMode);
        this.valuesSource = valuesSource;
        this.order = InternalOrder.validate(order, this);
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        this.minDocCount = minDocCount;
        bucketOrds = new LongHash(estimatedBucketCount, aggregationContext.bigArrays());
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        values = valuesSource.longValues();
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        final int valuesCount = values.setDocument(doc);

        if(passNumber>0){
            //Repeat pass - only delegate to child aggs for buckets that survived first pass pruning
            for (int i = 0; i < valuesCount; ++i) {
                final long val = values.nextValue();
                long bucketOrdinal = bucketOrds.find(val);                
                if (bucketDocCount(bucketOrdinal) != PRUNED_BUCKET) {
                    collectBucketNoCounts(doc, bucketOrdinal);
                }
            }
        } else {
            //First pass - create buckets and delegate to child aggs
            for (int i = 0; i < valuesCount; ++i) {
                final long val = values.nextValue();
                long bucketOrdinal = bucketOrds.add(val);
                if (bucketOrdinal < 0) { // already seen
                    bucketOrdinal = - 1 - bucketOrdinal;
                }
                collectBucket(doc, bucketOrdinal);
            }
        }
    }
    
    
    

    @Override
    protected void doPostCollection() {
        super.doPostCollection();
        
        if(passNumber>0){
            //We have already completed the bucket pruning in an earlier pass
            return;
        }        
        //Prune the bucketOrds to the top matching ones
        if (minDocCount == 0 && (order != InternalOrder.COUNT_DESC || bucketOrds.size() < requiredSize)) {
            // we need to fill-in the blanks
            for (AtomicReaderContext ctx : context.searchContext().searcher().getTopReaderContext().leaves()) {
                context.setNextReader(ctx);
                final LongValues values = valuesSource.longValues();
                for (int docId = 0; docId < ctx.reader().maxDoc(); ++docId) {
                    final int valueCount = values.setDocument(docId);
                    for (int i = 0; i < valueCount; ++i) {
                        bucketOrds.add(values.nextValue());
                    }
                }
            }
        }

        final int size = (int) Math.min(bucketOrds.size(), shardSize);
        prunedBuckets = new BucketPriorityQueue(size, order.comparator(this));
        LongTerms.Bucket spare = null;
        for (long i = 0; i < bucketOrds.capacity(); ++i) {
            final long ord = bucketOrds.id(i);
            if (ord < 0) {
                // slot is not allocated
                continue;
            }

            if (spare == null) {
                spare = new LongTerms.Bucket(0, 0, null);
            }
            spare.term = bucketOrds.key(i);
            spare.docCount = bucketDocCount(ord);
            spare.bucketOrd = ord;
            spare = (LongTerms.Bucket) prunedBuckets.insertWithOverflow(spare);
            if (spare != null) {
                //Pick up buckets that don't make the final cut and mark the ordinal as pruned.
                clearDocCount(spare.bucketOrd);
            }
        }        
    }
    BucketPriorityQueue prunedBuckets;


    @Override
    public LongTerms buildAggregation(long owningBucketOrdinal) {
        assert owningBucketOrdinal == 0;
        
        List<InternalTerms.Bucket> list;
        if (prunedBuckets == null) {
            list = new ArrayList<InternalTerms.Bucket>(0);
        } else {
            //Pruning already occurred in doPostCollection method - empty the PQ
            list = new ArrayList<InternalTerms.Bucket>(prunedBuckets.size());
            for (int i = prunedBuckets.size() - 1; i >= 0; --i) {
                   final LongTerms.Bucket bucket = (LongTerms.Bucket) prunedBuckets.pop();
                  list.add(new LongTerms.Bucket(bucket.term,  bucket.docCount, bucketAggregations(bucket.bucketOrd)));      
            } 
        }       
        
        return new LongTerms(name, order, valuesSource.formatter(), requiredSize, minDocCount, list);
    }

    @Override
    public LongTerms buildEmptyAggregation() {
        return new LongTerms(name, order, valuesSource.formatter(), requiredSize, minDocCount, Collections.<InternalTerms.Bucket>emptyList());
    }

    @Override
    public void doRelease() {
        Releasables.release(bucketOrds);
    }

}
