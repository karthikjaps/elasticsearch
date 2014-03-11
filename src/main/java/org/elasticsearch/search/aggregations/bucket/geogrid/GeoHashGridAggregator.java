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
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.numeric.NumericValuesSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

/**
 * Aggregates data expressed as GeoHash longs (for efficiency's sake) but formats results as Geohash strings.
 */

public class GeoHashGridAggregator extends BucketsAggregator {

    private static final int INITIAL_CAPACITY = 50; // TODO sizing

    private final int requiredSize;
    private final int shardSize;
    private final NumericValuesSource valuesSource;
    private final LongHash bucketOrds;
    private LongValues values;


    public GeoHashGridAggregator(String name, AggregatorFactories factories, NumericValuesSource valuesSource,
                                 int requiredSize, int shardSize, AggregationContext aggregationContext, Aggregator parent, ExecutionMode executionMode) {
        super(name, BucketAggregationMode.PER_BUCKET, factories, INITIAL_CAPACITY, aggregationContext, parent, executionMode);
        this.valuesSource = valuesSource;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        bucketOrds = new LongHash(INITIAL_CAPACITY, aggregationContext.bigArrays());
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        values = valuesSource.longValues();
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        final int valuesCount = values.setDocument(doc);

        if (passNumber > 0) {
            //Repeat pass - only delegate to child aggs for buckets that survived first pass pruning
            for (int i = 0; i < valuesCount; ++i) {
                final long val = values.nextValue();
                long bucketOrdinal = bucketOrds.find(val);
                if (bucketDocCount(bucketOrdinal) != PRUNED_BUCKET) {
                    collectBucketNoCounts(doc, bucketOrdinal);
                }
            }
        } else {
            for (int i = 0; i < valuesCount; ++i) {
                final long val = values.nextValue();
                long bucketOrdinal = bucketOrds.add(val);
                if (bucketOrdinal < 0) { // already seen
                    bucketOrdinal = -1 - bucketOrdinal;
                }
                collectBucket(doc, bucketOrdinal);
            }
        }
    }

    // private impl that stores a bucket ord. This allows for computing the aggregations lazily.
    static class OrdinalBucket extends InternalGeoHashGrid.Bucket {

        long bucketOrd;

        public OrdinalBucket() {
            super(0, 0, (InternalAggregations) null);
        }

    }


    InternalGeoHashGrid.BucketPriorityQueue pruned;

    @Override
    protected void doPostCollection() {
        super.doPostCollection();

        if (passNumber > 0) {
            return;  // Already pruned
        }
        final int size = (int) Math.min(bucketOrds.size(), shardSize);

        pruned = new InternalGeoHashGrid.BucketPriorityQueue(size);
        OrdinalBucket spare = null;
        for (long i = 0; i < bucketOrds.capacity(); ++i) {
            final long ord = bucketOrds.id(i);
            if (ord < 0) {
                // slot is not allocated
                continue;
            }

            if (spare == null) {
                spare = new OrdinalBucket();
            }
            spare.geohashAsLong = bucketOrds.key(i);
            spare.docCount = bucketDocCount(ord);
            spare.bucketOrd = ord;
            spare = (OrdinalBucket) pruned.insertWithOverflow(spare);
            if (spare != null) {
                //Pick up buckets that don't make the final cut and mark the ordinal as pruned.
                clearDocCount(spare.bucketOrd);
            }
        }

    }

    @Override
    public InternalGeoHashGrid buildAggregation(long owningBucketOrdinal) {
        assert owningBucketOrdinal == 0;

        final InternalGeoHashGrid.Bucket[] list;
        if (pruned == null) {
            list = new InternalGeoHashGrid.Bucket[0];
        } else {
            list = new InternalGeoHashGrid.Bucket[pruned.size()];
            for (int i = pruned.size() - 1; i >= 0; i--) {
                final OrdinalBucket bucket = (OrdinalBucket) pruned.pop();
                bucket.aggregations = bucketAggregations(bucket.bucketOrd);
                list[i] = bucket;
            }
        }

        return new InternalGeoHashGrid(name, requiredSize, Arrays.asList(list));
    }

    @Override
    public InternalGeoHashGrid buildEmptyAggregation() {
        return new InternalGeoHashGrid(name, requiredSize, Collections.<InternalGeoHashGrid.Bucket>emptyList());
    }


    @Override
    public void doRelease() {
        Releasables.release(bucketOrds);
    }

    public static class Unmapped extends Aggregator {
        private int requiredSize;

        public Unmapped(String name, int requiredSize, AggregationContext aggregationContext, Aggregator parent) {

            super(name, BucketAggregationMode.PER_BUCKET, AggregatorFactories.EMPTY, 0, aggregationContext, parent, ExecutionMode.SINGLE_PASS);
            this.requiredSize = requiredSize;
        }

        @Override
        public boolean shouldCollect() {
            return false;
        }

        @Override
        public void setNextReader(AtomicReaderContext reader) {
        }

        @Override
        public void collect(int doc, long owningBucketOrdinal) throws IOException {
        }

        @Override
        public InternalGeoHashGrid buildAggregation(long owningBucketOrdinal) {
            return buildEmptyAggregation();
        }

        @Override
        public InternalGeoHashGrid buildEmptyAggregation() {
            return new InternalGeoHashGrid(name, requiredSize, Collections.<InternalGeoHashGrid.Bucket>emptyList());
        }
    }

}
