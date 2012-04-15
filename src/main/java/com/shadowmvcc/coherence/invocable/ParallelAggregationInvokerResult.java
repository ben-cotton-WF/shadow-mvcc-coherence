package com.shadowmvcc.coherence.invocable;

import java.util.Map;

import com.shadowmvcc.coherence.domain.VersionedKey;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.net.partition.PartitionSet;

/**
 * Result type from a {@code ParallelAggregationInvoker}. Encapsulates a 
 * partial aggregation result, the set of partitions processed, and
 * a map of entries not aggregated because of uncommitted versions
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> the logical cache key
 * @param <R> the aggregator result type
 */
@Portable
public class ParallelAggregationInvokerResult<K, R> {

    public static final int POF_PARTITIONS = 0;
    @PortableProperty(POF_PARTITIONS)
    private PartitionSet partitions;
    public static final int POF_RESULT = 1;
    @PortableProperty(POF_RESULT)
    private R result;
    public static final int POF_RETRY = 2;
    @PortableProperty(POF_RETRY)
    private Map<K, VersionedKey<K>> retryMap;

    /**
     *  Default constructor for POF use only.
     */
    public ParallelAggregationInvokerResult() {
        super();
    }

    /**
     * @param partitions the set of partitions processed
     * @param result the partial aggregation result
     * @param retryMap the uncommitted entries
     */
    public ParallelAggregationInvokerResult(final PartitionSet partitions, 
            final R result, final Map<K, VersionedKey<K>> retryMap) {
        super();
        this.partitions = partitions;
        this.result = result;
        this.retryMap = retryMap;
    }

    /**
     * @return the set of partitions processed
     */
    public PartitionSet getPartitions() {
        return partitions;
    }

    /**
     * @return the partial invocation result
     */
    public R getResult() {
        return result;
    }

    /**
     * @return the map of uncommitted versions key is logical key, value is version cache key
     */
    public Map<K, VersionedKey<K>> getRetryMap() {
        return retryMap;
    }

}
