package com.sixwhits.cohmvcc.invocable;

import java.util.Map;

import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.net.partition.PartitionSet;

/**
 * Result type from an {@code EntryProcessorInvoker} that encapsulates
 * the map of {@code EntryProcessor} results, the map of entries
 * that could not be processed because of uncommitted changes,
 * and the set of partitions processed.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> the logical cache key
 * @param <R> the {@code EntryProcessor} result type
 */
@Portable
public class EntryProcessorInvokerResult<K, R> {

    public static final int POF_PARTITIONS = 0;
    @PortableProperty(POF_PARTITIONS)
    private PartitionSet partitions;
    public static final int POF_RESULTS = 1;
    @PortableProperty(POF_RESULTS)
    private Map<K, R> resultMap;
    public static final int POF_RETRY = 2;
    @PortableProperty(POF_RETRY)
    private Map<K, VersionedKey<K>> retryMap;

    /**
     *  Default constructor for POF use only.
     */
    public EntryProcessorInvokerResult() {
        super();
    }

    /**
     * @param partitions the set of partitions processed
     * @param resultMap the entry processor results
     * @param retryMap the uncommitted entries
     */
    public EntryProcessorInvokerResult(final PartitionSet partitions, 
            final Map<K, R> resultMap, final Map<K, VersionedKey<K>> retryMap) {
        super();
        this.partitions = partitions;
        this.resultMap = resultMap;
        this.retryMap = retryMap;
    }

    /**
     * @return the set of partitions processed
     */
    public PartitionSet getPartitions() {
        return partitions;
    }

    /**
     * @return the {@code EntryProcessor} results
     */
    public Map<K, R> getResultMap() {
        return resultMap;
    }

    /**
     * @return the map of uncommitted versions key is logical key, value is version cache key
     */
    public Map<K, VersionedKey<K>> getRetryMap() {
        return retryMap;
    }

}
