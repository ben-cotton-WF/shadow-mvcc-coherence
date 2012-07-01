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

package com.shadowmvcc.coherence.cache.internal;

import static com.shadowmvcc.coherence.domain.IsolationLevel.readProhibited;
import static com.shadowmvcc.coherence.domain.IsolationLevel.readUncommitted;
import static com.shadowmvcc.coherence.domain.IsolationLevel.repeatableRead;
import static com.shadowmvcc.coherence.domain.IsolationLevel.serializable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.cache.MVCCTransactionalCache;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.ProcessorResult;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.VersionCacheKey;
import com.shadowmvcc.coherence.domain.VersionedKey;
import com.shadowmvcc.coherence.index.MVCCSurfaceFilter;
import com.shadowmvcc.coherence.invocable.AggregatorWrapper;
import com.shadowmvcc.coherence.invocable.DecorationExtractorProcessor;
import com.shadowmvcc.coherence.invocable.EntryProcessorInvoker;
import com.shadowmvcc.coherence.invocable.EntryProcessorInvokerResult;
import com.shadowmvcc.coherence.invocable.FilterValidateEntryProcessor;
import com.shadowmvcc.coherence.invocable.InvocationServiceHelper;
import com.shadowmvcc.coherence.invocable.InvocationServiceHelper.InvocableFactory;
import com.shadowmvcc.coherence.invocable.MVCCEntryProcessorWrapper;
import com.shadowmvcc.coherence.invocable.MVCCReadOnlyEntryProcessorWrapper;
import com.shadowmvcc.coherence.invocable.ParallelAggregationInvoker;
import com.shadowmvcc.coherence.invocable.ParallelAggregationInvokerResult;
import com.shadowmvcc.coherence.invocable.ParallelAwareAggregatorWrapper;
import com.shadowmvcc.coherence.invocable.ParallelKeyAggregationInvoker;
import com.shadowmvcc.coherence.transaction.internal.ExistenceCheckProcessor;
import com.shadowmvcc.coherence.transaction.internal.ReadMarkingProcessor;
import com.shadowmvcc.coherence.transaction.internal.TransactionCache;
import com.shadowmvcc.coherence.transaction.internal.TransactionCacheImpl;
import com.shadowmvcc.coherence.transaction.internal.TransactionExpiryListener;
import com.shadowmvcc.coherence.utils.MapUtils;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.DistributedCacheService;
import com.tangosol.net.Invocable;
import com.tangosol.net.Member;
import com.tangosol.net.NamedCache;
import com.tangosol.net.partition.KeyPartitioningStrategy;
import com.tangosol.net.partition.PartitionSet;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.EntryAggregator;
import com.tangosol.util.InvocableMap.EntryProcessor;
import com.tangosol.util.InvocableMap.ParallelAwareAggregator;
import com.tangosol.util.aggregator.Count;
import com.tangosol.util.extractor.IdentityExtractor;
import com.tangosol.util.filter.EqualsFilter;
import com.tangosol.util.processor.ExtractorProcessor;

/**
 * Local implementation of {@link MVCCTransactionalCache}.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> cache key type
 * @param <V> cache value type
 */
public class MVCCTransactionalCacheImpl<K, V> extends AbstractMVCCTransactionalCache<K, V>
        implements MVCCTransactionalCache<K, V> {

    private final TransactionCache transactionCache;
    private final String invocationServiceName;

    /**
     * Constructor.
     * @param cacheName the logical cache name
     * @param invocationServiceName name of the invocation service to use.
     */
    public MVCCTransactionalCacheImpl(final String cacheName, final String invocationServiceName) {
        super(cacheName);
        this.transactionCache = new TransactionCacheImpl(invocationServiceName);
        this.invocationServiceName = invocationServiceName;
    }

    @Override
    public V get(final TransactionId tid, final IsolationLevel isolationLevel, final K key) {
        EntryProcessor ep = new MVCCReadOnlyEntryProcessorWrapper<K, V>(
                tid, new ExtractorProcessor(new IdentityExtractor()), isolationLevel, cacheName);
        return invokeUntilCommitted(key, tid, ep);
    }

    @Override
    public V put(final TransactionId tid, final IsolationLevel isolationLevel,
            final boolean autoCommit, final K key, final V value) {
        EntryProcessor ep = new MVCCEntryProcessorWrapper<K, V>(
                tid, new UnconditionalPutProcessor(value, true), isolationLevel, autoCommit, cacheName);
        return invokeUntilCommitted(key, tid, ep);
    }

    @Override
    public void insert(final TransactionId tid, final boolean autoCommit, final K key, final V value) {
        EntryProcessor ep = new MVCCEntryProcessorWrapper<K, Object>(
                tid, new UnconditionalPutProcessor(value, false), readProhibited, autoCommit, cacheName);
        invokeUntilCommitted(key, tid, ep);
    }

    @Override
    public V remove(final TransactionId tid, final IsolationLevel isolationLevel,
            final boolean autoCommit, final K key) {
        EntryProcessor ep = new MVCCEntryProcessorWrapper<K, V>(
                tid, new UnconditionalRemoveProcessor(), isolationLevel, autoCommit, cacheName);
        return invokeUntilCommitted(key, tid, ep);
    }

    @Override
    public <R> InvocationFinalResult<K, R> invoke(final TransactionId tid, final IsolationLevel isolationLevel,
            final boolean autoCommit, final boolean readonly, final K key, final EntryProcessor agent) {
        EntryProcessor ep;
        if (readonly) {
            ep = new MVCCReadOnlyEntryProcessorWrapper<K, R>(tid, agent, isolationLevel, cacheName);
        } else {
            ep = new MVCCEntryProcessorWrapper<K, R>(tid, agent, isolationLevel, autoCommit, cacheName);
        }
        return invokeProcessorUntilCommitted(key, tid, ep);
    }

    /**
     * Invoke an EntryProcessor on a key, waiting for a commit if necessary.
     * @param key the key
     * @param tid transaction id
     * @param ep the entry processor
     * @return the result of the EntryProcessor and changed keys wrapped in an InvocationFinalResult
     * @param <R> the EntryProcessor return type
     */
    private <R> InvocationFinalResult<K, R> invokeProcessorUntilCommitted(
            final K key, final TransactionId tid, final EntryProcessor ep) {
        while (true) {
            @SuppressWarnings("unchecked")
            ProcessorResult<K, R> epr = (ProcessorResult<K, R>) keyCache.invoke(key, ep);
            if (epr == null) {
                return null;
            }
            if (epr.isUncommitted()) {
                waitForCommit(epr.getWaitKey(), tid);
            } else {
                Map<K, R> resultMap = new HashMap<K, R>();
                resultMap.put(key, epr.getResult());
                InvocationFinalResult<K, R> result = new InvocationFinalResult<K, R>(
                        resultMap, epr.getChangedCacheKeys());
                return result;
            }
        }
    }
    
    /**
     * Invoke an EntryProcessor on a key, waiting for a commit if necessary.
     * @param key the key
     * @param tid transaction id
     * @param ep the entry processor
     * @return the result of the EntryProcessor
     * @param <R> the EntryProcessor return type
     */
    private <R> R invokeUntilCommitted(
            final K key, final TransactionId tid, final EntryProcessor ep) {
        while (true) {
            @SuppressWarnings("unchecked")
            ProcessorResult<K, R> epr = (ProcessorResult<K, R>) keyCache.invoke(key, ep);
            if (epr == null) {
                return null;
            }
            if (epr.isUncommitted()) {
                waitForCommit(epr.getWaitKey(), tid);
            } else {
                return epr.getResult();
            }
        }
    }

    /**
     * Wait for the specified uncommitted version cache entry to be committed or rolled back.
     * Throws TransactionException if the current transaction expires while waiting
     * @param awaitedKey the key to wait for
     * @param transactionId id of current transaction
     */
    private void waitForCommit(final VersionCacheKey<K> awaitedKey, final TransactionId transactionId) {
        NamedCache waitCache = CacheFactory.getCache(awaitedKey.getCacheName().getVersionCacheName());
        VersionCommitListener vcl = new VersionCommitListener();
        TransactionExpiryListener txl = new TransactionExpiryListener(vcl);
        try {
            waitCache.addMapListener(vcl, awaitedKey.getKey(), false);
            transactionCache.registerExpiryListener(transactionId, txl);
            Boolean committed = (Boolean) versionCache.invoke(
                    awaitedKey.getKey(), DecorationExtractorProcessor.COMMITTED_INSTANCE);
            if (committed != null && !committed) {
                vcl.waitForCommit();
            }
            return;
        } finally {
            waitCache.removeMapListener(vcl, awaitedKey.getKey());
            transactionCache.unregisterExpiryListener(transactionId, txl);
        }
    }

    @Override
    public int size(final TransactionId tid, final IsolationLevel isolationLevel) throws Throwable {
        return (Integer) aggregate(tid, isolationLevel, (Filter) null, new Count());
    }

    @Override
    public boolean isEmpty(final TransactionId tid, final IsolationLevel isolationLevel) throws Throwable {
        return size(tid, isolationLevel) == 0;
    }

    @Override
    public boolean containsKey(final TransactionId tid, final IsolationLevel isolationLevel, final K key) {
        EntryProcessor ep = new MVCCReadOnlyEntryProcessorWrapper<K, Boolean>(
                tid, new ExistenceCheckProcessor(), isolationLevel, cacheName);
        Boolean result = invokeUntilCommitted(key, tid, ep);
        return result == null ? false : result;
    }

    @Override
    public boolean containsValue(
            final TransactionId tid, final IsolationLevel isolationLevel, final V value) throws Throwable {
        EntryProcessor ep = new MVCCReadOnlyEntryProcessorWrapper<K, Boolean>(
                tid, new ExistenceCheckProcessor(), isolationLevel, cacheName);
        Filter filter = new EqualsFilter(IdentityExtractor.INSTANCE, value);
        // TODO optimise - we only need to find a single committed matching entry, even if others are uncommitted
        InvocationFinalResult<K, Boolean> fr = invokeAllUntilCommitted(filter, tid, ep);
        Map<K, Boolean> result = fr.getResultMap();
        for (Boolean exists : result.values()) {
            if (exists != null && exists) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void putAll(final TransactionId tid, final boolean autoCommit, final Map<K, V> m) {
        DistributedCacheService service = (DistributedCacheService) versionCache.getCacheService();
        KeyPartitioningStrategy partitionStrategy = service.getKeyPartitioningStrategy();

        Map<Integer, Map<K, V>> memberValueMap = new HashMap<Integer, Map<K, V>>();
        for (Map.Entry<K, V> entry : m.entrySet()) {
            int partition = partitionStrategy.getKeyPartition(entry.getKey());
            Integer member = service.getPartitionOwner(partition).getId();
            if (!memberValueMap.containsKey(member)) {
                memberValueMap.put(member, new HashMap<K, V>());
            }
            memberValueMap.get(member).put(entry.getKey(), entry.getValue());
        }

        // TODO run in parallel
        for (Member member : (Set<Member>) service.getOwnershipEnabledMembers()) {
            Map<K, V> valueMap = memberValueMap.get(member.getId());
            if (valueMap != null) {
                EntryProcessor putProcessor = new PutAllProcessor<K, V>(valueMap);
                EntryProcessor wrappedProcessor = new MVCCEntryProcessorWrapper<K, Object>(
                        tid, putProcessor, readProhibited, autoCommit, cacheName);
                invokeAllUntilCommitted(valueMap.keySet(), tid, wrappedProcessor);
            }
        }

    }

    @Override
    public void clear(final TransactionId tid, final boolean autoCommit) throws Throwable {
        EntryProcessor ep = new MVCCEntryProcessorWrapper<K, V>(
                tid, new UnconditionalRemoveProcessor(false), readProhibited, autoCommit, cacheName);

//        TODO: should we prohibit autocommit here?
//        if (autoCommit) {
//            throw new IllegalArgumentException("autocommit not permitted for clear");
//        }
        
        invokeAllUntilCommitted((Filter) null, tid, ep);
    }

    @Override
    public Set<K> keySet(final TransactionId tid, final IsolationLevel isolationLevel) throws Throwable {
        //TODO Wrap to add java.util.Map semantics to update underlying cache
        return keySet(tid, isolationLevel, null);
    }

    @Override
    public Collection<V> values(final TransactionId tid, final IsolationLevel isolationLevel) throws Throwable {
        //TODO find a more efficient implementation that doesn't require the complete map to be returned
        EntryProcessor ep = new MVCCReadOnlyEntryProcessorWrapper<K, V>(
                tid, new ExtractorProcessor(new IdentityExtractor()), isolationLevel, cacheName, null);
        InvocationFinalResult<K, V> fr = invokeAllUntilCommitted((Filter) null, tid, ep);
        return fr.getResultMap().values();
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet(
            final TransactionId tid, final IsolationLevel isolationLevel) throws Throwable {
        //TODO Wrap to add java.util.Map semantics to update underlying cache
        return entrySet(tid, isolationLevel, null);
    }

    @Override
    public Map<K, V> getAll(final TransactionId tid, final IsolationLevel isolationLevel, final Collection<K> colKeys) {
        EntryProcessor ep = new MVCCReadOnlyEntryProcessorWrapper<K, V>(
                tid, new ExtractorProcessor(new IdentityExtractor()), isolationLevel, cacheName);
        return invokeAllUntilCommitted(colKeys, tid, ep);
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet(final TransactionId tid,
            final IsolationLevel isolationLevel, final Filter filter) throws Throwable {
        EntryProcessor ep = new MVCCReadOnlyEntryProcessorWrapper<K, V>(
                tid, new ExtractorProcessor(new IdentityExtractor()), isolationLevel, cacheName, filter);
        InvocationFinalResult<K, V> fr = invokeAllUntilCommitted(filter, tid, ep);
        return fr.getResultMap().entrySet();
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet(final TransactionId tid, final IsolationLevel isolationLevel,
            final Filter filter, final Comparator<V> comparator) {
        // TODO Auto-generated method stub
        // TODO is the comparator on the value or the entry?
        return null;
    }

    @Override
    public Set<K> keySet(final TransactionId tid,
            final IsolationLevel isolationLevel, final Filter filter) throws Throwable {
        EntryProcessor ep = new MVCCReadOnlyEntryProcessorWrapper<K, Object>(
                tid, null, isolationLevel, cacheName, filter);
        InvocationFinalResult<K, Object> fr = invokeAllUntilCommitted(filter, tid, ep);
        return fr.getResultMap().keySet();
    }

    @Override
    public <R> R aggregate(final TransactionId tid, final IsolationLevel isolationLevel,
            final Collection<K> collKeys, final EntryAggregator agent) throws Throwable {

        if (agent instanceof ParallelAwareAggregator) {
            ParallelAwareAggregatorWrapper wrapper
                = new ParallelAwareAggregatorWrapper((ParallelAwareAggregator) agent);
            return aggregateParallel(tid, isolationLevel, collKeys, wrapper);
        } else {
            AggregatorWrapper wrapper = new AggregatorWrapper(agent);
            return aggregateSerial(tid, isolationLevel, collKeys, wrapper);
        }
    }

    @Override
    public <R> R aggregate(final TransactionId tid, final IsolationLevel isolationLevel,
            final Filter filter, final EntryAggregator agent) throws Throwable {
        if (agent instanceof ParallelAwareAggregator) {
            ParallelAwareAggregatorWrapper wrapper
                = new ParallelAwareAggregatorWrapper((ParallelAwareAggregator) agent);
            return aggregateParallel(tid, isolationLevel, filter, wrapper);
        } else {
            AggregatorWrapper wrapper = new AggregatorWrapper(agent);
            return aggregateSerial(tid, isolationLevel, filter, wrapper);
        }
    }

    /**
     * Perform a serial cache aggregation against a filter. TODO is this ever used?
     * @param tid transaction id
     * @param isolationLevel isolation level
     * @param filter filter
     * @param agent aggregator
     * @return the aggregation result
     * @param <R> aggregator result type
     * @throws Throwable if an invocation fails in the cluster
     */
    @SuppressWarnings("unchecked")
    private <R> R aggregateSerial(final TransactionId tid, final IsolationLevel isolationLevel,
            final Filter filter, final EntryAggregator agent) throws Throwable {
        if (isolationLevel == repeatableRead || isolationLevel == serializable) {
            invokeAllUntilCommitted(filter, tid, new ReadMarkingProcessor<K>(tid, isolationLevel, cacheName));
        }
        EntryAggregator wrapper;
        wrapper = new AggregatorWrapper(agent);
        // TODO restrict filter to include only items already marked read with this tid for repeatableRead
        MVCCSurfaceFilter<K> surfaceFilter = new MVCCSurfaceFilter<K>(tid, filter);
        return (R) versionCache.aggregate(surfaceFilter, wrapper);
    }

    /**
     * Perform a serial aggregation against a set of keys.
     * @param tid transaction id
     * @param isolationLevel isolation level
     * @param keys collection of keys to aggregate
     * @param agent the aggregator
     * @return the aggregation results
     * @param <R> aggregation result type
     */
    @SuppressWarnings("unchecked")
    private <R> R aggregateSerial(final TransactionId tid, final IsolationLevel isolationLevel,
            final Collection<K> keys, final EntryAggregator agent) {
        ReadMarkingProcessor<K> readMarker = new ReadMarkingProcessor<K>(tid, isolationLevel, cacheName, true);
        Map<K, ProcessorResult<K, VersionedKey<K>>> markMap =
                (Map<K, ProcessorResult<K, VersionedKey<K>>>) keyCache.invokeAll(
                    keys, readMarker);
        EntryAggregator wrapper;
        Set<VersionedKey<K>> vkeys = new HashSet<VersionedKey<K>>(markMap.size());
        for (Map.Entry<K, ProcessorResult<K, VersionedKey<K>>> entry : markMap.entrySet()) {
            ProcessorResult<K, VersionedKey<K>> pr = entry.getValue();
            while (isolationLevel != readUncommitted && pr != null && pr.isUncommitted()) {
                waitForCommit(pr.getWaitKey(), tid);
                pr = (ProcessorResult<K, VersionedKey<K>>) keyCache.invoke(entry.getKey(), readMarker);
            }
            if (pr != null) {
                vkeys.add(pr.getResult());
            }

        }
        wrapper = new AggregatorWrapper(agent);
        return (R) versionCache.aggregate(vkeys, wrapper);
    }

    /**
     * Perform a parallel cache aggregation against a filter. TODO is this ever used?
     * @param tid transaction id
     * @param isolationLevel isolation level
     * @param filter filter
     * @param agent aggregator
     * @return the aggregation result
     * @param <R> aggregator result type
     * @throws Throwable if an invocation fails
     */
    @SuppressWarnings("unchecked")
    private <R> R aggregateParallel(final TransactionId tid, final IsolationLevel isolationLevel,
            final Filter filter, final ParallelAwareAggregator agent) throws Throwable {
        DistributedCacheService service = (DistributedCacheService) versionCache.getCacheService();
        PartitionSet remainingPartitions = new PartitionSet(service.getPartitionCount());
        remainingPartitions.fill();

        Map<K, VersionCacheKey<K>> retryMap = new HashMap<K, VersionCacheKey<K>>();
        Collection<R> partialResults = new ArrayList<R>(service.getOwnershipEnabledMembers().size());
        
        InvocationServiceHelper<ParallelAggregationInvokerResult<K, R>> invocationServiceHelper =
                new InvocationServiceHelper<ParallelAggregationInvokerResult<K, R>>(invocationServiceName);
        
        InvocableFactory<PartitionSet> invocableFactory = new InvocableFactory<PartitionSet>() {
            @Override
            public Invocable getInvocable(final PartitionSet invocationTargetSet) {
                return new ParallelAggregationInvoker<K, R>(
                        cacheName, filter, tid, agent, isolationLevel, invocationTargetSet);
            }
        };
        
        invocationServiceHelper.invokeActionForPartitionSet(remainingPartitions, cacheName, invocableFactory);

        Collection<ParallelAggregationInvokerResult<K, R>> invocationResults =
                    invocationServiceHelper.waitForAllInvocations();

        for (ParallelAggregationInvokerResult<K, R> result : invocationResults) {
            partialResults.add(result.getResult());
            retryMap.putAll(result.getRetryMap());
        }

        if (retryMap.size() > 0) {
            Set<VersionedKey<K>> remnantKeys = new HashSet<VersionedKey<K>>();

            while (retryMap.size() > 0) {
                for (Map.Entry<K, VersionCacheKey<K>> entry : retryMap.entrySet()) {
                    waitForCommit(entry.getValue(), tid);
                }
                Set<K> retryKeys = new HashSet<K>(retryMap.keySet());
                retryMap.clear();

                for (Map.Entry<K, ProcessorResult<K, VersionedKey<K>>> entry
                        : ((Map<K, ProcessorResult<K, VersionedKey<K>>>) keyCache.invokeAll(retryKeys,
                                new FilterValidateEntryProcessor<K>(
                                        tid, isolationLevel, cacheName, filter))).entrySet()) {
                    if (entry.getValue().isUncommitted()) {
                        retryMap.put(entry.getKey(), entry.getValue().getWaitKey());
                    } else {
                        remnantKeys.add(entry.getValue().getResult());
                    }
                }
            }

            R retriedPartial = (R) versionCache.aggregate(remnantKeys, agent.getParallelAggregator());

            partialResults.add(retriedPartial);
        }

        R result = (R) agent.aggregateResults(partialResults);

        return result;

    }

    /**
     * Perform a parallel aggregation against a set of keys.
     * @param tid transaction id
     * @param isolationLevel isolation level
     * @param keys collection of keys to aggregate
     * @param agent the aggregator
     * @return the aggregation results
     * @param <R> aggregation result type
     * @throws Throwable if an invocation fails
     */
    @SuppressWarnings("unchecked")
    private <R> R aggregateParallel(final TransactionId tid, final IsolationLevel isolationLevel,
            final Collection<K> keys, final ParallelAwareAggregator agent) throws Throwable {
        DistributedCacheService service = (DistributedCacheService) versionCache.getCacheService();
        PartitionSet remainingPartitions = new PartitionSet(service.getPartitionCount());
        remainingPartitions.fill();

        Map<K, VersionCacheKey<K>> retryMap = new HashMap<K, VersionCacheKey<K>>();
        Collection<R> partialResults = new ArrayList<R>(service.getOwnershipEnabledMembers().size());
        
        InvocationServiceHelper<ParallelAggregationInvokerResult<K, R>> invocationHelper =
                new InvocationServiceHelper<ParallelAggregationInvokerResult<K, R>>(invocationServiceName);

        InvocableFactory<PartitionSet> invocableFactory = new InvocableFactory<PartitionSet>() {

            @Override
            public Invocable getInvocable(final PartitionSet invocationTargetSet) {
                return new ParallelKeyAggregationInvoker<K, R>(
                        cacheName, keys, tid, agent, isolationLevel, invocationTargetSet);
            }
            
        };
        
        invocationHelper.invokeActionForPartitionSet(remainingPartitions, cacheName, invocableFactory);
        
        Collection<ParallelAggregationInvokerResult<K, R>> invocationResults =
                    invocationHelper.waitForAllInvocations();

        for (ParallelAggregationInvokerResult<K, R> result : invocationResults) {
            if (result != null) {
                partialResults.add(result.getResult());
                retryMap.putAll(result.getRetryMap());
            }
        }

        if (retryMap.size() > 0) {
            Set<VersionedKey<K>> remnantKeys = new HashSet<VersionedKey<K>>();

            for (Map.Entry<K, VersionCacheKey<K>> entry : retryMap.entrySet()) {

                ProcessorResult<K, VersionedKey<K>> pr = null;
                ReadMarkingProcessor<K> readMarker =
                        new ReadMarkingProcessor<K>(tid, isolationLevel, cacheName, true);
                VersionCacheKey<K> waitKey = entry.getValue();
                do {
                    waitForCommit(waitKey, tid);
                    pr = (ProcessorResult<K, VersionedKey<K>>) keyCache.invoke(entry.getKey(), readMarker);
                    if (pr != null) {
                        waitKey = pr.getWaitKey();
                    }
                } while (pr != null && pr.isUncommitted());
                if (pr != null) {
                    remnantKeys.add(pr.getResult());
                }
            }

            R retriedPartial = (R) versionCache.aggregate(remnantKeys, agent.getParallelAggregator());

            partialResults.add(retriedPartial);
        }

        R result = (R) agent.aggregateResults(partialResults);

        return result;

    }

    /**
     * Invoke an EntryProcessor against a filter, waiting for any commits to complete
     * and retrying as necessary.
     * @param filter the filter
     * @param tid transaction id
     * @param entryProcessor the entryProcessor
     * @return an invocation result including results and changed keys.
     * @param <R> EntryProcessor result type
     * @throws Throwable if an invocation fails
     */
    @SuppressWarnings("unchecked")
    private <R> InvocationFinalResult<K, R> invokeAllUntilCommitted(final Filter filter, final TransactionId tid, 
            final EntryProcessor entryProcessor) throws Throwable {

        DistributedCacheService service = (DistributedCacheService) versionCache.getCacheService();
        PartitionSet remainingPartitions = new PartitionSet(service.getPartitionCount());
        remainingPartitions.fill();

        Map<K, VersionCacheKey<K>> retryMap = new HashMap<K, VersionCacheKey<K>>();
        Map<K, R> resultMap = new HashMap<K, R>();
        Map<CacheName, Set<Object>> changedKeys = new HashMap<CacheName, Set<Object>>();
        
        InvocationServiceHelper<EntryProcessorInvokerResult<K, R>> invocationHelper =
                new InvocationServiceHelper<EntryProcessorInvokerResult<K, R>>(invocationServiceName);
        InvocableFactory<PartitionSet> invocableFactory = new InvocableFactory<PartitionSet>() {

            @Override
            public Invocable getInvocable(final PartitionSet invocationTargetSet) {
                return new EntryProcessorInvoker<K, R>(
                        cacheName, filter, tid, entryProcessor, invocationTargetSet);
            }
            
        };
        
        invocationHelper.invokeActionForPartitionSet(remainingPartitions, cacheName, invocableFactory);
        
        Collection<EntryProcessorInvokerResult<K, R>> invocationResults = invocationHelper.waitForAllInvocations();

        for (EntryProcessorInvokerResult<K, R> result : invocationResults) {
            resultMap.putAll(result.getResultMap());
            retryMap.putAll(result.getRetryMap());
            for (Map.Entry<CacheName, Set<Object>> ckEntry : result.getChangedKeys().entrySet()) {
                CacheName cacheName = ckEntry.getKey();
                if (!changedKeys.containsKey(cacheName)) {
                    changedKeys.put(cacheName, new HashSet<Object>());
                }
                changedKeys.get(cacheName).addAll(ckEntry.getValue());
            }
        }

        for (Map.Entry<K, VersionCacheKey<K>> entry : retryMap.entrySet()) {
            waitForCommit(entry.getValue(), tid);
            while (true) {
                ProcessorResult<K, R> epr = (ProcessorResult<K, R>) keyCache.invoke(entry.getKey(), entryProcessor);
                if (epr == null) {
                    break;
                }
                if (!epr.isUncommitted()) {
                    resultMap.put(entry.getKey(), epr.getResult());
                    break;
                }
                waitForCommit(epr.getWaitKey(), tid);
            }
        }

        return new InvocationFinalResult<K, R>(resultMap, changedKeys);
    }
    
    /**
     * Invoke an EntryProcessor against a collection of keys, waiting for any commits to complete
     * and retrying as necessary.
     * @param keys the collection of keys
     * @param tid transaction id
     * @param entryProcessor the entryProcessor
     * @return the map of EntryProcessor results.
     * @param <R> EntryProcessor result type
     */
    @SuppressWarnings("unchecked")
    private <R> Map<K, R> invokeAllUntilCommitted(final Collection<K> keys, final TransactionId tid, 
            final EntryProcessor entryProcessor) {

        Map<K, VersionCacheKey<K>> retryMap = new HashMap<K, VersionCacheKey<K>>();
        Map<K, R> resultMap = new HashMap<K, R>();

        // TODO would it be more efficient to use invocation service for this?
        
        for (Map.Entry<K, ProcessorResult<K, R>> entry
                : ((Map<K, ProcessorResult<K, R>>) keyCache.invokeAll(keys, entryProcessor)).entrySet()) {
            if (entry.getValue().isUncommitted()) {
                retryMap.put(entry.getKey(), entry.getValue().getWaitKey());
            } else {
                resultMap.put(entry.getKey(), entry.getValue().getResult());
            }
        }

        for (Map.Entry<K, VersionCacheKey<K>> entry : retryMap.entrySet()) {
            waitForCommit(entry.getValue(), tid);
            while (true) {
                ProcessorResult<K, R> epr = (ProcessorResult<K, R>) keyCache.invoke(entry.getKey(), entryProcessor);
                if (epr == null) {
                    break;
                }
                if (!epr.isUncommitted()) {
                    resultMap.put(entry.getKey(), epr.getResult());
                    break;
                }
                waitForCommit(epr.getWaitKey(), tid);
            }
        }

        return resultMap;
    }

    /**
     * Invoke a wrapped entry processor returning an InvocationFinalResult.
     * incorporating return values and changed keys
     * @param keys keys to invoke on
     * @param tid transaction id
     * @param entryProcessor entry processor
     * @return an InvocationFinalResult
     * @param <R> EntryProcessor return type
     */
    @SuppressWarnings("unchecked")
    private <R> InvocationFinalResult<K, R> invokeAllProcessorUntilCommitted(
            final Collection<K> keys, final TransactionId tid, 
            final EntryProcessor entryProcessor) {

        Map<K, VersionCacheKey<K>> retryMap = new HashMap<K, VersionCacheKey<K>>();
        Map<K, R> resultMap = new HashMap<K, R>();
        Map<CacheName, Set<Object>> changedKeys = new HashMap<CacheName, Set<Object>>();

        // TODO would it be more efficient to use invocation service for this?
        
        for (Map.Entry<K, ProcessorResult<K, R>> entry
                : ((Map<K, ProcessorResult<K, R>>) keyCache.invokeAll(keys, entryProcessor)).entrySet()) {
            if (entry.getValue().isUncommitted()) {
                retryMap.put(entry.getKey(), entry.getValue().getWaitKey());
            } else {
                resultMap.put(entry.getKey(), entry.getValue().getResult());
                MapUtils.mergeSets(changedKeys, entry.getValue().getChangedCacheKeys());
            }
        }

        for (Map.Entry<K, VersionCacheKey<K>> entry : retryMap.entrySet()) {
            waitForCommit(entry.getValue(), tid);
            while (true) {
                ProcessorResult<K, R> epr = (ProcessorResult<K, R>) keyCache.invoke(entry.getKey(), entryProcessor);
                if (epr == null) {
                    break;
                }
                if (!epr.isUncommitted()) {
                    resultMap.put(entry.getKey(), epr.getResult());
                    MapUtils.mergeSets(changedKeys, epr.getChangedCacheKeys());
                    break;
                }
                waitForCommit(epr.getWaitKey(), tid);
            }
        }

        return new InvocationFinalResult<K, R>(resultMap, changedKeys);
    }

    /**
     * {@inheritDoc}.
     * 
     * @throws IllegalArgumentException if called with autocommit set as it is not possible to guarantee
     * atomic completion of invocation against all keys
     */
    @Override
    public <R> InvocationFinalResult<K, R> invokeAll(final TransactionId tid, final IsolationLevel isolationLevel,
            final boolean autoCommit, final boolean readOnly,
            final Collection<K> collKeys, final EntryProcessor agent) {
        
        if (autoCommit) {
            throw new IllegalArgumentException("autocommit not permitted for invokeAll");
        }
        
        EntryProcessor wrappedProcessor;
        if (readOnly) {
            wrappedProcessor = new MVCCReadOnlyEntryProcessorWrapper<K, R>(tid, agent, isolationLevel, cacheName);
        } else {
            wrappedProcessor = new MVCCEntryProcessorWrapper<K, R>(
                tid, agent, isolationLevel, autoCommit, cacheName);
        }
        return invokeAllProcessorUntilCommitted(collKeys, tid, wrappedProcessor);
    }

    /**
     * {@inheritDoc}.
     * @throws Throwable 
     * 
     * @throws IllegalArgumentException if called with autocommit set as it is not possible to guarantee
     * atomic completion of invocation against all keys
     */
    @Override
    public <R> InvocationFinalResult<K, R> invokeAll(final TransactionId tid, final IsolationLevel isolationLevel,
            final boolean autoCommit, final boolean readOnly,
            final Filter filter, final EntryProcessor agent) throws Throwable {

        if (autoCommit) {
            throw new IllegalArgumentException("autocommit not permitted for invokeAll");
        }
        
        EntryProcessor wrappedProcessor;
        if (readOnly) {
            wrappedProcessor = new MVCCReadOnlyEntryProcessorWrapper<K, R>(
                    tid, agent, isolationLevel, cacheName, filter);
            
        } else {
            wrappedProcessor = new MVCCEntryProcessorWrapper<K, R>(
                    tid, agent, isolationLevel, autoCommit, cacheName, filter);
        }
        return invokeAllUntilCommitted(filter, tid, wrappedProcessor);
    }

}
