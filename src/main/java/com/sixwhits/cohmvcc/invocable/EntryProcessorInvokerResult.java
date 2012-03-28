package com.sixwhits.cohmvcc.invocable;

import java.util.Map;
import java.util.Set;

import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.pof.SetCodec;
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

    @PortableProperty(0) private PartitionSet partitions;
    @PortableProperty(1) private Map<K, R> resultMap;
    @PortableProperty(2) private Map<K, VersionedKey<K>> retryMap;
    @PortableProperty(value = 3, codec = SetCodec.class) private Set<K> changedKeys;

    /**
     *  Default constructor for POF use only.
     */
    public EntryProcessorInvokerResult() {
        super();
    }

    /**
     * Constructor.
     * @param partitions the set of partitions processed
     * @param resultMap the entry processor results
     * @param retryMap the uncommitted entries
     * @param changedKeys the set of keys changed by this invocation
     */
    public EntryProcessorInvokerResult(final PartitionSet partitions, 
            final Map<K, R> resultMap, final Map<K, VersionedKey<K>> retryMap,
            final Set<K> changedKeys) {
        super();
        this.partitions = partitions;
        this.resultMap = resultMap;
        this.retryMap = retryMap;
        this.changedKeys = changedKeys;
    }

    /**
     * Get the set of partitions that were processed by the invocation.
     * @return the set of partitions processed
     */
    public PartitionSet getPartitions() {
        return partitions;
    }

    /**
     * Get the result map linking logical key to the individual {@code EntryProcessor} results.
     * @return the {@code EntryProcessor} results
     */
    public Map<K, R> getResultMap() {
        return resultMap;
    }

    /**
     * Get the map of entries that must be retried because of an uncommitted change. The key of the
     * map is the logical key, the value is the version cache key of the uncommitted entry found.
     * @return the map of uncommitted versions
     */
    public Map<K, VersionedKey<K>> getRetryMap() {
        return retryMap;
    }

    /**
     * Get the set of keys for entries that were changed by this invocation.
     * @return the set of keys for changed entries
     */
    public Set<K> getChangedKeys() {
        return changedKeys;
    }

}
