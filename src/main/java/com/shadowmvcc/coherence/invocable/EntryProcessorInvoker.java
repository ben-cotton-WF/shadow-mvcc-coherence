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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.domain.ProcessorResult;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.VersionedKey;
import com.shadowmvcc.coherence.index.MVCCSurfaceFilter;
import com.shadowmvcc.coherence.processor.Reducer;
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
import com.tangosol.util.InvocableMap.EntryProcessor;
import com.tangosol.util.filter.PartitionedFilter;

/**
 * {@code Invocable} to execute an EntryProcessor against a filter on a specific member. Can operate
 * on a specified set of partitions, or an the set of partitions local to the member it runs on.
 * The result contains the map of {@link EntryProcessor} results, a map of entries for which invocation
 * could not be performed because of uncommitted changes, and the set of partitions processed.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> Key type of the cache
 * @param <R> Result type of the EntryProcessor
 */
@Portable
public class EntryProcessorInvoker<K, R> implements Invocable {

    private static final long serialVersionUID = 4499295487465987875L;

    @PortableProperty(0) private CacheName cacheName;
    @PortableProperty(1) private Filter filter;
    @PortableProperty(2) private TransactionId tid;
    @PortableProperty(3) private EntryProcessor entryProcessor;
    @PortableProperty(4) private PartitionSet partitions = null;

    private transient PartitionSet memberParts;
    private transient Map<K, R> resultMap;
    private transient Map<K, VersionedKey<K>> retryMap;
    private transient Set<K> changedKeys;

    /**
     * Default constructor for POF use only.
     */
    public EntryProcessorInvoker() {
        super();
    }

    /**
     * Constructor without partitions. Will execute on partitions local to the member.
     * @param cacheName cache name
     * @param filter the filter
     * @param tid current transaction id
     * @param entryProcessor the EntryProcessor to invoke
     */
    public EntryProcessorInvoker(final CacheName cacheName, final Filter filter, 
            final TransactionId tid, final EntryProcessor entryProcessor) {
        super();
        this.cacheName = cacheName;
        this.filter = filter;
        this.tid = tid;
        this.entryProcessor = entryProcessor;
    }

    /**
     * Constructor specifying which partitions .
     * @param cacheName cache name
     * @param filter the filter
     * @param tid current transaction id
     * @param entryProcessor the EntryProcessor to invoke
     * @param partitions the set of partitions to inboke on
     */
    public EntryProcessorInvoker(final CacheName cacheName, final Filter filter, 
            final TransactionId tid, final EntryProcessor entryProcessor, 
            final PartitionSet partitions) {
        super();
        this.cacheName = cacheName;
        this.filter = filter;
        this.tid = tid;
        this.entryProcessor = entryProcessor;
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

        Set<VersionedKey<K>> vkeys = versionCache.keySet(filterPart);
        Set<K> keys = new HashSet<K>();
        for (VersionedKey<K> vk: vkeys) {
            keys.add(vk.getNativeKey());
        }

        retryMap = new HashMap<K, VersionedKey<K>>();
        resultMap = new HashMap<K, R>();
        changedKeys = new HashSet<K>();

        for (Map.Entry<K, ProcessorResult<K, R>> entry
                : ((Map<K, ProcessorResult<K, R>>) keyCache.invokeAll(keys, entryProcessor)).entrySet()) {
            ProcessorResult<K, R> result = entry.getValue();
            if (result.isUncommitted()) {
                retryMap.put(entry.getKey(), result.getWaitKey());
            } else {
                if (result.isReturnResult()) {
                    resultMap.put(entry.getKey(), result.getResult());
                }
                if (result.isChanged()) {
                    changedKeys.add(entry.getKey());
                }
            }
        }
        
        if (entryProcessor instanceof Reducer) {
            resultMap = ((Reducer) entryProcessor).reduce(resultMap);
        }
    }

    @Override
    public EntryProcessorInvokerResult<K, R> getResult() {
        return new EntryProcessorInvokerResult<K, R>(memberParts, resultMap, retryMap, changedKeys);
    }

}
