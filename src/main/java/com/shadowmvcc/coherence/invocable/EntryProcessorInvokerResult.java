/*

Copyright 2012 Shadowmist Ltd.

This file is part of Shadow MVCC for Oracle Coherence.

Shadow MVCC for Oracle Coherence is free software: you can redistribute 
it and/or modify it under the terms of the GNU General Public License 
as published by the Free Software Foundation, either version 3 of the 
License, or (at your option) any later version.

Shadow MVCC for Oracle Coherence is distributed in the hope that it 
will be useful, but WITHOUT ANY WARRANTY; without even the implied 
warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See 
the GNU General Public License for more details.
                        
You should have received a copy of the GNU General Public License
along with Shadow MVCC for Oracle Coherence.  If not, see 
<http://www.gnu.org/licenses/>.

*/

package com.shadowmvcc.coherence.invocable;

import java.util.Map;
import java.util.Set;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.domain.VersionCacheKey;
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
    @PortableProperty(2) private Map<K, VersionCacheKey<K>> retryMap;
    @PortableProperty(3) private Map<CacheName, Set<Object>> changedKeys;

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
            final Map<K, R> resultMap, final Map<K, VersionCacheKey<K>> retryMap,
            final Map<CacheName, Set<Object>> changedKeys) {
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
    public Map<K, VersionCacheKey<K>> getRetryMap() {
        return retryMap;
    }

    /**
     * Get the set of keys for entries that were changed by this invocation.
     * @return the set of keys for changed entries
     */
    public Map<CacheName, Set<Object>> getChangedKeys() {
        return changedKeys;
    }

}
