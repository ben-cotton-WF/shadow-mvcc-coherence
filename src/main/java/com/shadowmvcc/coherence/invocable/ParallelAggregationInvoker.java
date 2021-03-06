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

import static com.shadowmvcc.coherence.domain.IsolationLevel.readUncommitted;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.ProcessorResult;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.VersionCacheKey;
import com.shadowmvcc.coherence.domain.VersionedKey;
import com.shadowmvcc.coherence.index.MVCCSurfaceFilter;
import com.shadowmvcc.coherence.transaction.internal.ReadMarkingProcessor;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.DistributedCacheService;
import com.tangosol.net.Invocable;
import com.tangosol.net.InvocationService;
import com.tangosol.net.Member;
import com.tangosol.net.NamedCache;
import com.tangosol.net.partition.PartitionSet;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.ParallelAwareAggregator;
import com.tangosol.util.filter.PartitionedFilter;

/**
 * {@code Invocable} to perform partial aggregation against a filter on a specified set
 * of partitions, or the partitions owned by the current member.
 * 
 * Invocation will mark entries as read if required by the isolation level. The result
 * will be a {@link ParallelAggregationInvokerResult} containing a partial aggregation
 * of the matching filter, with a map of those keys that could not be aggregated because
 * of an uncommitted change. 
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> the logical cache key
 * @param <R> the result type of the aggregation
 */
@Portable
public class ParallelAggregationInvoker<K, R> implements Invocable {

    private static final long serialVersionUID = 4499295487465987875L;

    public static final int POF_CACHENAME = 0;
    @PortableProperty(POF_CACHENAME)
    private CacheName cacheName;
    public static final int POF_FILTER = 1;
    @PortableProperty(POF_FILTER)
    private Filter filter;
    public static final int POF_TS = 2;
    @PortableProperty(POF_TS)
    private TransactionId tid;
    public static final int POF_AGGREGATOR = 3;
    @PortableProperty(POF_AGGREGATOR)
    private ParallelAwareAggregator aggregator;
    public static final int POF_PARTITIONS = 4;
    @PortableProperty(POF_PARTITIONS)
    private PartitionSet partitions = null;
    public static final int POF_ISOLATION = 5;
    @PortableProperty(POF_ISOLATION)
    private IsolationLevel isolationLevel;

    private transient PartitionSet memberParts;
    private transient R aggregationResult;
    private transient Map<K, VersionCacheKey<K>> retryMap;

    /**
     *  Default constructor for POF use only.
     */
    public ParallelAggregationInvoker() {
        super();
    }

    /**
     * Constructor with no specified partitions. Will execute against partitions
     * owned by the current member.
     * @param cacheName the cache name
     * @param filter the filter
     * @param tid the transaction id
     * @param aggregator the aggregator
     * @param isolationLevel the isolation level
     */
    public ParallelAggregationInvoker(final CacheName cacheName, final Filter filter, 
            final TransactionId tid, final ParallelAwareAggregator aggregator, final IsolationLevel isolationLevel) {
        super();
        this.cacheName = cacheName;
        this.filter = filter;
        this.tid = tid;
        this.aggregator = aggregator;
        this.isolationLevel = isolationLevel;
    }

    /**
     * Constructor specifying the partitions to aggregate.
     * @param cacheName the cache name
     * @param filter the filter
     * @param tid the transaction id
     * @param aggregator the aggregator
     * @param isolationLevel the isolation level
     * @param partitions partition set to aggregate
     */
    public ParallelAggregationInvoker(final CacheName cacheName, final Filter filter, 
            final TransactionId tid, final ParallelAwareAggregator aggregator, final IsolationLevel isolationLevel, 
            final PartitionSet partitions) {
        super();
        this.cacheName = cacheName;
        this.filter = filter;
        this.tid = tid;
        this.aggregator = aggregator;
        this.isolationLevel = isolationLevel;
        this.partitions = partitions;
    }

    @Override
    public void init(final InvocationService invocationservice) {
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {

        NamedCache versionCache = CacheFactory.getCache(cacheName.getVersionCacheName());
        NamedCache keyCache = CacheFactory.getCache(cacheName.getKeyCacheName());
        DistributedCacheService cacheService = (DistributedCacheService) versionCache.getCacheService();
        Member thisMember = CacheFactory.getCluster().getLocalMember();

        memberParts = cacheService.getOwnedPartitions(thisMember);
        if (partitions != null) {
            memberParts.retain(partitions);
        }

        MVCCSurfaceFilter<K> surfaceFilter = new MVCCSurfaceFilter<K>(tid, filter);
        Filter filterPart = new PartitionedFilter(surfaceFilter, memberParts);
        Set<VersionedKey<K>> candidateVkeys = versionCache.keySet(filterPart);
        Set<K> candidateKeys = new HashSet<K>();
        for (VersionedKey<K> vkey : candidateVkeys) {
            candidateKeys.add(vkey.getLogicalKey());
        }
        Map<K, ProcessorResult<K, VersionedKey<K>>> markMap =
                (Map<K, ProcessorResult<K, VersionedKey<K>>>) keyCache.invokeAll(
                candidateKeys, new ReadMarkingProcessor<K>(tid, isolationLevel, cacheName, true));

        Set<VersionedKey<K>> vkeys = new HashSet<VersionedKey<K>>();

        retryMap = new HashMap<K, VersionCacheKey<K>>();

        for (Map.Entry<K, ProcessorResult<K, VersionedKey<K>>> entry : markMap.entrySet()) {
            if (isolationLevel != readUncommitted && entry.getValue().isUncommitted()) {
                retryMap.put(entry.getKey(), entry.getValue().getWaitKey());
            } else {
                vkeys.add(entry.getValue().getResult());
            }
        }

        aggregationResult = (R) versionCache.aggregate(vkeys, aggregator.getParallelAggregator());
    }

    @Override
    public ParallelAggregationInvokerResult<K, R> getResult() {
        return new ParallelAggregationInvokerResult<K, R>(aggregationResult, retryMap);
    }

}
