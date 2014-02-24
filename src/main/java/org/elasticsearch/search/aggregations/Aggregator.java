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
package org.elasticsearch.search.aggregations;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.ReaderContextAware;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.*;

public abstract class Aggregator implements Releasable, ReaderContextAware {

    /**
     * Defines the nature of the aggregator's aggregation execution when nested in other aggregators and the buckets they create.
     */
    public static enum BucketAggregationMode {

        /**
         * In this mode, a new aggregator instance will be created per bucket (created by the parent aggregator)
         */
        PER_BUCKET,

        /**
         * In this mode, a single aggregator instance will be created per parent aggregator, that will handle the aggregations of all its buckets.
         */
        MULTI_BUCKETS
    }
    
    public enum ExecutionMode {
        /**
         * Creates buckets and delegates to child aggregators in a single pass over
         * the matching documents
         */
        SINGLE_PASS(new ParseField("single_pass")),


        /**
         * Creates buckets for all matching docs and then prunes to top-scoring buckets
         * before a second pass over the data when child aggregators are called 
         * but only for the top-scoring docs
         */
        PRUNE_FIRST(new ParseField("prune_first"));
        
        private final ParseField parseField;

        ExecutionMode (ParseField parseField) {
            this.parseField = parseField;
        }

        public ParseField parseField() {
            return parseField;
        }

        public static ExecutionMode parse(String value) {
            return parse(value, ParseField.EMPTY_FLAGS);
        }

        public static ExecutionMode parse(String value, EnumSet<ParseField.Flag> flags) {
            ExecutionMode[] values = ExecutionMode.values();
            ExecutionMode type = null;
            for (ExecutionMode t : values) {
                if (t.parseField().match(value, flags)) {
                    type = t;
                    break;
                }
            }
            if (type == null) {
                throw new ElasticsearchParseException("No executionMode found for value: " + value);
            }
            return type;
        }
    }     

    protected final String name;
    protected final Aggregator parent;
    protected final AggregationContext context;
    protected final BigArrays bigArrays;
    protected final int depth;
    protected final long estimatedBucketCount;

    protected final BucketAggregationMode bucketAggregationMode;
    protected final AggregatorFactories factories;
    protected Aggregator[] subAggregators=null;
    protected final ExecutionMode executionMode;
    protected int passNumber=0;
    private static final Aggregator[] EMPTY_AGGREGATORS = new Aggregator[0];

    private Map<String, Aggregator> subAggregatorbyName;

    /**
     * Constructs a new Aggregator.
     *
     * @param name                  The name of the aggregation
     * @param bucketAggregationMode The nature of execution as a sub-aggregator (see {@link BucketAggregationMode})
     * @param factories             The factories for all the sub-aggregators under this aggregator
     * @param estimatedBucketsCount When served as a sub-aggregator, indicate how many buckets the parent aggregator will generate.
     * @param context               The aggregation context
     * @param parent                The parent aggregator (may be {@code null} for top level aggregators)
     * @param executionMode         The nature of execution as a parent (see {@link ExecutionMode} )
     */
    protected Aggregator(String name, BucketAggregationMode bucketAggregationMode, AggregatorFactories factories, long estimatedBucketsCount, AggregationContext context, Aggregator parent, ExecutionMode executionMode) {
        this.name = name;
        this.parent = parent;
        this.estimatedBucketCount = estimatedBucketsCount;
        this.context = context;
        this.bigArrays = context.bigArrays();
        this.depth = parent == null ? 0 : 1 + parent.depth();
        this.bucketAggregationMode = bucketAggregationMode;
        assert factories != null : "sub-factories provided to BucketAggregator must not be null, use AggragatorFactories.EMPTY instead";
        this.factories = factories;
        this.executionMode=executionMode;
        if (executionMode == ExecutionMode.PRUNE_FIRST) {
            //First pass we don't have any sub aggregators we need to call   
            this.subAggregators = EMPTY_AGGREGATORS;
            //TODO the above logic is over-simplistic. Some pruneFirst aggs may need some of
            // the child aggs around to compute stats that are consulted in this agg's pruning logic.
            // Need to delegate to subclass and ask which subAggs are required for the first pass
            // BUT.... can't do that here because this is a constructor and any subclasses are
            // not yet fully instantiated so we can't call abstract methods yet. Need to re-think
            // when and where required child aggs are set up.
        } else {
            this.subAggregators = factories.createSubAggregators(this, estimatedBucketsCount);
        }
        
    }

    /**
     * @return  The name of the aggregation.
     */
    public String name() {
        return name;
    }
    
    /** Return true if this or any child aggregation implements a PRUNE_FIRST {@link ExecutionMode}  
     * which requires all content to be streamed to first find top buckets and then re-streamed 
     * to populate child aggregations for only the buckets surviving round one.*/
    public boolean requiresMatchReplays() {
        if(executionMode == ExecutionMode.PRUNE_FIRST){
            return true;
        }
        for (int i = 0; i < subAggregators.length; i++) {
            if(subAggregators[i].requiresMatchReplays()){
                return true;
            }
        }
        return false;
    }
    

    /** Return the estimated number of buckets. */
    public final long estimatedBucketCount() {
        return estimatedBucketCount;
    }

    /** Return the depth of this aggregator in the aggregation tree. */
    public final int depth() {
        return depth;
    }

    /**
     * @return  The parent aggregator of this aggregator. The addAggregation are hierarchical in the sense that some can
     *          be composed out of others (more specifically, bucket addAggregation can define other addAggregation that will
     *          be aggregated per bucket). This method returns the direct parent aggregator that contains this aggregator, or
     *          {@code null} if there is none (meaning, this aggregator is a top level one)
     */
    public Aggregator parent() {
        return parent;
    }

    public Aggregator[] subAggregators() {
        return subAggregators;
    }

    public Aggregator subAggregator(String aggName) {
        if (subAggregatorbyName == null) {
            subAggregatorbyName = new HashMap<String, Aggregator>(subAggregators.length);
            for (int i = 0; i < subAggregators.length; i++) {
                subAggregatorbyName.put(subAggregators[i].name, subAggregators[i]);
            }
        }
        return subAggregatorbyName.get(aggName);
    }

    /**
     * @return  The current aggregation context.
     */
    public AggregationContext context() {
        return context;
    }

    /**
     * @return  The bucket aggregation mode of this aggregator. This mode defines the nature in which the aggregation is executed
     * @see     BucketAggregationMode
     */
    public BucketAggregationMode bucketAggregationMode() {
        return bucketAggregationMode;
    }

    /**
     * @return  Whether this aggregator is in the state where it can collect documents. 
     *          
     *          Aggregations can require multiple passes over the matching data if {@link ExecutionMode} is configured
     *          to defer computing child aggregations until a parent has pruned a potentially large set  of candidate buckets.
     *          A passNumber variable is used to count the number of passes that have occurred over the result set.
     *          When passNumber is zero aggs typically return true to collect docs.
     *          When passNumber is >zero aggs will only return true if any child aggs have yet to be instantiated and collect docs.
     *          In replay mode (passNumber>zero) any parent agg should just delegate collect calls on to child aggs without
     *          updating any doc counts or similar state they may have stored in the first pass.
     *          
     *          Some aggregators can do their aggregations without actually collecting documents, for example, an aggregator 
     *          that computes stats over unmapped fields doesn't need to collect
     *          anything as it knows to just return "empty" stats as the aggregation result.
     *          
     */
    public boolean shouldCollect() {
        boolean result=false;
        if(passNumber == 0) { 
            //All aggs collect at least once (the exception being unmapped which overrides this method)
            result = true;
        } else {
            //Parent aggs will need to delegate collect calls to any deferred children
            for (int i = 0; i < subAggregators.length; i++) {
                if (subAggregators[i].shouldCollect()) {
                    result=true;
                    break;
                }
            }
        }
        return result;
    }    

    /**
     * Called during the query phase, to collect & aggregate the given document.
     *
     * @param doc                   The document to be collected/aggregated
     * @param owningBucketOrdinal   The ordinal of the bucket this aggregator belongs to, assuming this aggregator is not a top level aggregator.
     *                              Typically, aggregators with {@code #bucketAggregationMode} set to {@link BucketAggregationMode#MULTI_BUCKETS}
     *                              will heavily depend on this ordinal. Other aggregators may or may not use it and can see this ordinal as just
     *                              an extra information for the aggregation context. For top level aggregators, the ordinal will always be
     *                              equal to 0.
     * @throws IOException
     */
    public abstract void collect(int doc, long owningBucketOrdinal) throws IOException;

    /**
     * Called after collection of all document is done.
     */
    public final void postCollection() {
        for (int i = 0; i < subAggregators.length; i++) {
            subAggregators[i].postCollection();
        }
        doPostCollection();
        if (passNumber==0) {
            if (executionMode == ExecutionMode.PRUNE_FIRST) {
                // This is the end of the first pass and this agg's content should now have been pruned
                // in the doPostCollection method.
                // Now set up child aggregators ready for subsequent "replay" passes over the content
                this.subAggregators = factories.createSubAggregators(this, estimatedBucketCount);
                prepareSubAggregators();
            }
        }
        passNumber++;
    }

    /** Called when deferred sub aggregators need to be established ready for replays of content following
     * a pruneFirst {@link ExecutionMode} to building the aggregations tree*/
    protected void prepareSubAggregators() {
        
    }

    /** Called upon release of the aggregator. */
    @Override
    public boolean release() {
        boolean success = false;
        try {
            doRelease();
            success = true;
        } finally {
            Releasables.release(success, subAggregators);
        }
        return true;
    }

    /** Release instance-specific data. */
    protected void doRelease() {}

    /**
     * Can be overriden by aggregator implementation to be called back when the collection phase ends.
     */
    protected void doPostCollection() {
    }

    /**
     * @return  The aggregated & built aggregation
     */
    public abstract InternalAggregation buildAggregation(long owningBucketOrdinal);

    public abstract InternalAggregation buildEmptyAggregation();

    protected final InternalAggregations buildEmptySubAggregations() {
        List<InternalAggregation> aggs = new ArrayList<InternalAggregation>();
        for (Aggregator aggregator : subAggregators) {
            aggs.add(aggregator.buildEmptyAggregation());
        }
        return new InternalAggregations(aggs);
    }

    /**
     * Parses the aggregation request and creates the appropriate aggregator factory for it.
     *
     * @see {@link AggregatorFactory}
    */
    public static interface Parser {

        /**
         * @return The aggregation type this parser is associated with.
         */
        String type();

        /**
         * Returns the aggregator factory with which this parser is associated, may return {@code null} indicating the
         * aggregation should be skipped (e.g. when trying to aggregate on unmapped fields).
         *
         * @param aggregationName   The name of the aggregation
         * @param parser            The xcontent parser
         * @param context           The search context
         * @return                  The resolved aggregator factory or {@code null} in case the aggregation should be skipped
         * @throws java.io.IOException      When parsing fails
         */
        AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException;

    }

}
