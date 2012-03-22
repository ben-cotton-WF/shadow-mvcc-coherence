package com.sixwhits.cohmvcc.cache.internal;

import static com.sixwhits.cohmvcc.domain.Constants.KEYEXTRACTOR;
import static com.sixwhits.cohmvcc.domain.IsolationLevel.readProhibited;
import static com.sixwhits.cohmvcc.domain.IsolationLevel.readUncommitted;
import static com.sixwhits.cohmvcc.domain.IsolationLevel.repeatableRead;
import static com.sixwhits.cohmvcc.domain.IsolationLevel.serializable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.cache.MVCCTransactionalCache;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.ProcessorResult;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.event.MVCCEventFilter;
import com.sixwhits.cohmvcc.event.MVCCEventTransformer;
import com.sixwhits.cohmvcc.event.MVCCMapListener;
import com.sixwhits.cohmvcc.index.MVCCExtractor;
import com.sixwhits.cohmvcc.index.MVCCSurfaceFilter;
import com.sixwhits.cohmvcc.invocable.AggregatorWrapper;
import com.sixwhits.cohmvcc.invocable.DecorationExtractorProcessor;
import com.sixwhits.cohmvcc.invocable.EntryProcessorInvoker;
import com.sixwhits.cohmvcc.invocable.EntryProcessorInvokerResult;
import com.sixwhits.cohmvcc.invocable.FilterValidateEntryProcessor;
import com.sixwhits.cohmvcc.invocable.MVCCEntryProcessorWrapper;
import com.sixwhits.cohmvcc.invocable.MVCCReadOnlyEntryProcessorWrapper;
import com.sixwhits.cohmvcc.invocable.ParallelAggregationInvoker;
import com.sixwhits.cohmvcc.invocable.ParallelAggregationInvokerResult;
import com.sixwhits.cohmvcc.invocable.ParallelAwareAggregatorWrapper;
import com.sixwhits.cohmvcc.invocable.ParallelKeyAggregationInvoker;
import com.sixwhits.cohmvcc.transaction.internal.ExistenceCheckProcessor;
import com.sixwhits.cohmvcc.transaction.internal.ReadMarkingProcessor;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.CacheService;
import com.tangosol.net.DistributedCacheService;
import com.tangosol.net.InvocationService;
import com.tangosol.net.Member;
import com.tangosol.net.NamedCache;
import com.tangosol.net.partition.KeyPartitioningStrategy;
import com.tangosol.net.partition.PartitionSet;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.EntryAggregator;
import com.tangosol.util.InvocableMap.EntryProcessor;
import com.tangosol.util.InvocableMap.ParallelAwareAggregator;
import com.tangosol.util.MapEventTransformer;
import com.tangosol.util.MapListener;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.aggregator.Count;
import com.tangosol.util.extractor.IdentityExtractor;
import com.tangosol.util.filter.AlwaysFilter;
import com.tangosol.util.filter.EqualsFilter;
import com.tangosol.util.filter.MapEventTransformerFilter;
import com.tangosol.util.processor.ExtractorProcessor;

/**
 * Local implementation of {@link MVCCTransactionalCacheImpl}.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> cache key type
 * @param <V> cache value type
 */
public class MVCCTransactionalCacheImpl<K, V> implements MVCCTransactionalCache<K, V> {

    private final NamedCache keyCache;
    private final NamedCache versionCache;
    private final CacheName cacheName;
    private final InvocationService invocationService;

    /**
     * Key class for the local map of MapListeners. Containing the supplied
     * listener and the key or filter parameter used to register (or null for whole cache)
     */
    private static class ListenerMapKey {
        private final MapListener listener;
        private final Object param;
        /**
         * Constructor.
         * @param listener the user's listener
         * @param param the filter or key specifier, or null
         */
        public ListenerMapKey(final MapListener listener, final Object param) {
            super();
            this.listener = listener;
            this.param = param;
        }
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                    + ((listener == null) ? 0 : listener.hashCode());
            result = prime * result + ((param == null) ? 0 : param.hashCode());
            return result;
        }
        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            ListenerMapKey other = (ListenerMapKey) obj;
            if (listener == null) {
                if (other.listener != null) {
                    return false;
                }
            } else if (listener != other.listener) {
                return false;
            }
            if (param == null) {
                if (other.param != null) {
                    return false;
                }
            } else if (!param.equals(other.param)) {
                return false;
            }
            return true;
        }

    }
    private final ConcurrentMap<ListenerMapKey, MVCCMapListener<K, V>> listenerMap
            = new ConcurrentHashMap<ListenerMapKey, MVCCMapListener<K, V>>();

    /**
     * Constructor.
     * @param cacheName the logical cache name
     * @param invocationServiceName name of the invocation service to use.
     */
    public MVCCTransactionalCacheImpl(final String cacheName, final String invocationServiceName) {
        super();
        this.cacheName = new CacheName(cacheName);
        this.keyCache = CacheFactory.getCache(this.cacheName.getKeyCacheName());
        this.versionCache = CacheFactory.getCache(this.cacheName.getVersionCacheName());
        versionCache.addIndex(MVCCExtractor.INSTANCE, false, null);
        this.invocationService = (InvocationService) CacheFactory.getService(invocationServiceName);
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
    public <R> R invoke(final TransactionId tid, final IsolationLevel isolationLevel,
            final boolean autoCommit, final K key, final EntryProcessor agent) {
        EntryProcessor ep = new MVCCEntryProcessorWrapper<K, R>(tid, agent, isolationLevel, autoCommit, cacheName);
        return invokeUntilCommitted(key, tid, ep);
    }

    /**
     * Invoke an EntryProcessor on a key, waiting for a commit if necessary.
     * @param key the key
     * @param tid transaction id
     * @param ep the entry processor
     * @return the result of the EntryProcessor
     * @param <R> the EntryProcessor return type
     */
    private <R> R invokeUntilCommitted(final K key, final TransactionId tid, final EntryProcessor ep) {
        while (true) {
            @SuppressWarnings("unchecked")
            ProcessorResult<K, R> epr = (ProcessorResult<K, R>) keyCache.invoke(key, ep);
            if (epr == null) {
                return null;
            }
            if (epr.isUncommitted()) {
                waitForCommit(epr.getWaitKey());
            } else {
                return epr.getResult();
            }
        }
    }

    /**
     * Wait for the specified uncommitted version cache entry to be committed or rolled back.
     * @param awaitedKey the key to wait for
     */
    private void waitForCommit(final VersionedKey<K> awaitedKey) {
        VersionCommitListener vcl = new VersionCommitListener();
        try {
            versionCache.addMapListener(vcl, awaitedKey, false);
            Boolean committed = (Boolean) versionCache.invoke(
                    awaitedKey, DecorationExtractorProcessor.COMMITTED_INSTANCE);
            if (committed != null && !committed) {
                vcl.waitForCommit();
            }
            return;
        } finally {
            versionCache.removeMapListener(vcl, awaitedKey);
        }
    }

    @Override
    public void addMapListener(final MapListener listener, final TransactionId tid,
            final IsolationLevel isolationLevel) {
        MVCCMapListener<K, V> mvccml = new MVCCMapListener<K, V>(listener);
        if (listenerMap.putIfAbsent(new ListenerMapKey(listener, null), mvccml) == null) {
            versionCache.addMapListener(mvccml, 
                    new MapEventTransformerFilter(AlwaysFilter.INSTANCE,
                            new MVCCEventTransformer<K, V>(isolationLevel, tid, cacheName)), false);
        }
    }

    @Override
    public void addMapListener(final MapListener listener, final TransactionId tid,
            final IsolationLevel isolationLevel, final Object oKey, final boolean fLite) {
        Filter keyFilter = new EqualsFilter(KEYEXTRACTOR, oKey);
        MVCCMapListener<K, V> mvccml = new MVCCMapListener<K, V>(listener);
        if (listenerMap.putIfAbsent(new ListenerMapKey(listener, oKey), mvccml) == null) {
            versionCache.addMapListener(mvccml, 
                    new MapEventTransformerFilter(keyFilter,
                            new MVCCEventTransformer<K, V>(isolationLevel, tid, cacheName)), false);
        }
    }

    @Override
    public void addMapListener(final MapListener listener, final TransactionId tid,
            final IsolationLevel isolationLevel, final Filter filter, final boolean fLite) {
        MVCCMapListener<K, V> mvccml = new MVCCMapListener<K, V>(listener);
        if (listenerMap.putIfAbsent(new ListenerMapKey(listener, filter), mvccml) == null) {
            MapEventTransformer transformer = new MVCCEventTransformer<K, V>(isolationLevel, tid, cacheName);
            Filter eventfilter = new MVCCEventFilter<K>(isolationLevel, filter, cacheName, transformer);
            versionCache.addMapListener(mvccml, eventfilter, false);
        }
    }

    @Override
    public void removeMapListener(final MapListener listener) {
        ListenerMapKey lmk = new ListenerMapKey(listener, null);
        versionCache.removeMapListener(listenerMap.get(lmk));
        listenerMap.remove(lmk);
    }

    @Override
    public void removeMapListener(final MapListener listener, final Object oKey) {
        ListenerMapKey lmk = new ListenerMapKey(listener, oKey);
        versionCache.removeMapListener(listenerMap.get(lmk), oKey);
        listenerMap.remove(lmk);
    }

    @Override
    public void removeMapListener(final MapListener listener, final Filter filter) {
        ListenerMapKey lmk = new ListenerMapKey(listener, filter);
        versionCache.removeMapListener(listenerMap.get(lmk), filter);
        listenerMap.remove(lmk);
    }

    @Override
    public int size(final TransactionId tid, final IsolationLevel isolationLevel) {
        return (Integer) aggregate(tid, isolationLevel, (Filter) null, new Count());
    }

    @Override
    public boolean isEmpty(final TransactionId tid, final IsolationLevel isolationLevel) {
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
    public boolean containsValue(final TransactionId tid, final IsolationLevel isolationLevel, final V value) {
        EntryProcessor ep = new MVCCReadOnlyEntryProcessorWrapper<K, Boolean>(
                tid, new ExistenceCheckProcessor(), isolationLevel, cacheName);
        Filter filter = new EqualsFilter(IdentityExtractor.INSTANCE, value);
        // TODO optimise - we only need to find a single committed matching entry, even if others are uncommitted
        Map<K, Boolean> result = invokeAllUntilCommitted(filter, tid, ep);
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
            EntryProcessor putProcessor = new PutAllProcessor<K, V>(valueMap);
            EntryProcessor wrappedProcessor = new MVCCEntryProcessorWrapper<K, Object>(
                    tid, putProcessor, readProhibited, autoCommit, cacheName);
            invokeAllUntilCommitted(valueMap.keySet(), tid, wrappedProcessor);
        }

    }

    @Override
    public void clear(final TransactionId tid, final boolean autoCommit) {
        EntryProcessor ep = new MVCCEntryProcessorWrapper<K, V>(
                tid, new UnconditionalRemoveProcessor(false), readProhibited, autoCommit, cacheName);
        invokeAllUntilCommitted((Filter) null, tid, ep);
    }

    @Override
    public Set<K> keySet(final TransactionId tid, final IsolationLevel isolationLevel) {
        //TODO Wrap to add java.util.Map semantics to update underlying cache
        return keySet(tid, isolationLevel, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<V> values(final TransactionId tid, final IsolationLevel isolationLevel) {
        //TODO find a more efficient implementation that doesn't require the complete map to be returned
        EntryProcessor ep = new MVCCReadOnlyEntryProcessorWrapper<K, V>(
                tid, new ExtractorProcessor(new IdentityExtractor()), isolationLevel, cacheName, null);
        return ((Map<K, V>) invokeAllUntilCommitted((Filter) null, tid, ep)).values();
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet(final TransactionId tid, final IsolationLevel isolationLevel) {
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
    public void addIndex(final ValueExtractor extractor, final boolean fOrdered, 
            final Comparator<V> comparator) {
        //TODO index on key extractor not correctly supported
        versionCache.addIndex(extractor, fOrdered, comparator);

    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<Map.Entry<K, V>> entrySet(final TransactionId tid,
            final IsolationLevel isolationLevel, final Filter filter) {
        EntryProcessor ep = new MVCCReadOnlyEntryProcessorWrapper<K, V>(
                tid, new ExtractorProcessor(new IdentityExtractor()), isolationLevel, cacheName, filter);
        return ((Map<K, V>) invokeAllUntilCommitted(filter, tid, ep)).entrySet();
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet(final TransactionId tid, final IsolationLevel isolationLevel,
            final Filter filter, final Comparator<V> comparator) {
        // TODO Auto-generated method stub
        // TODO is the comparator on the value or the entry?
        return null;
    }

    @Override
    public Set<K> keySet(final TransactionId tid, final IsolationLevel isolationLevel, final Filter filter) {
        EntryProcessor ep = new MVCCReadOnlyEntryProcessorWrapper<K, Object>(
                tid, null, isolationLevel, cacheName, filter);
        return ((Map<K, Object>) invokeAllUntilCommitted(filter, tid, ep)).keySet();
    }

    @Override
    public void removeIndex(final ValueExtractor extractor) {
        versionCache.removeIndex(extractor);
    }

    @Override
    public <R> R aggregate(final TransactionId tid, final IsolationLevel isolationLevel,
            final Collection<K> collKeys, final EntryAggregator agent) {

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
            final Filter filter, final EntryAggregator agent) {
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
     */
    @SuppressWarnings("unchecked")
    private <R> R aggregateSerial(final TransactionId tid, final IsolationLevel isolationLevel,
            final Filter filter, final EntryAggregator agent) {
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
                waitForCommit(pr.getWaitKey());
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
     */
    @SuppressWarnings("unchecked")
    private <R> R aggregateParallel(final TransactionId tid, final IsolationLevel isolationLevel,
            final Filter filter, final ParallelAwareAggregator agent) {
        DistributedCacheService service = (DistributedCacheService) versionCache.getCacheService();
        PartitionSet remainingPartitions = new PartitionSet(service.getPartitionCount());
        remainingPartitions.fill();

        Map<K, VersionedKey<K>> retryMap = new HashMap<K, VersionedKey<K>>();
        Collection<R> partialResults = new ArrayList<R>(service.getOwnershipEnabledMembers().size());

        do {
            ParallelAggregationInvoker<K, R> invoker = new ParallelAggregationInvoker<K, R>(
                    cacheName, filter, tid, agent, isolationLevel, remainingPartitions);

            Collection<ParallelAggregationInvokerResult<K, R>> invocationResults =
                    invocationService.query(invoker, service.getOwnershipEnabledMembers()).values();

            for (ParallelAggregationInvokerResult<K, R> result : invocationResults) {
                partialResults.add(result.getResult());
                retryMap.putAll(result.getRetryMap());
                remainingPartitions.remove(result.getPartitions());
            }

        } while (!remainingPartitions.isEmpty());

        if (retryMap.size() > 0) {
            Set<VersionedKey<K>> remnantKeys = new HashSet<VersionedKey<K>>();

            while (retryMap.size() > 0) {
                for (Map.Entry<K, VersionedKey<K>> entry : retryMap.entrySet()) {
                    waitForCommit(entry.getValue());
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
     */
    @SuppressWarnings("unchecked")
    private <R> R aggregateParallel(final TransactionId tid, final IsolationLevel isolationLevel,
            final Collection<K> keys, final ParallelAwareAggregator agent) {
        DistributedCacheService service = (DistributedCacheService) versionCache.getCacheService();
        PartitionSet remainingPartitions = new PartitionSet(service.getPartitionCount());
        remainingPartitions.fill();

        Map<K, VersionedKey<K>> retryMap = new HashMap<K, VersionedKey<K>>();
        Collection<R> partialResults = new ArrayList<R>(service.getOwnershipEnabledMembers().size());

        do {
            ParallelKeyAggregationInvoker<K, R> invoker = new ParallelKeyAggregationInvoker<K, R>(
                    cacheName, keys, tid, agent, isolationLevel, remainingPartitions);

            Collection<ParallelAggregationInvokerResult<K, R>> invocationResults =
                    invocationService.query(invoker, service.getOwnershipEnabledMembers()).values();

            for (ParallelAggregationInvokerResult<K, R> result : invocationResults) {
                if (result != null) {
                    partialResults.add(result.getResult());
                    retryMap.putAll(result.getRetryMap());
                    remainingPartitions.remove(result.getPartitions());
                }
            }

        } while (!remainingPartitions.isEmpty());

        if (retryMap.size() > 0) {
            Set<VersionedKey<K>> remnantKeys = new HashSet<VersionedKey<K>>();

            for (Map.Entry<K, VersionedKey<K>> entry : retryMap.entrySet()) {

                if (isolationLevel == readUncommitted) {
                    remnantKeys.add(entry.getValue());
                } else {
                    ProcessorResult<K, VersionedKey<K>> pr = null;
                    ReadMarkingProcessor<K> readMarker =
                            new ReadMarkingProcessor<K>(tid, isolationLevel, cacheName, true);
                    VersionedKey<K> waitKey = entry.getValue();
                    do {
                        waitForCommit(waitKey);
                        pr = (ProcessorResult<K, VersionedKey<K>>) keyCache.invoke(entry.getKey(), readMarker);
                        if (pr != null) {
                            waitKey = pr.getWaitKey();
                        }
                    } while (pr != null && pr.isUncommitted());
                    if (pr != null) {
                        remnantKeys.add(pr.getResult());
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
     * Invoke an EntryProcessor against a filter, waiting for any commits to complete
     * and retrying as necessary.
     * @param filter the filter
     * @param tid transaction id
     * @param entryProcessor the entryProcessor
     * @return the map of EntryProcessor results.
     * @param <R> EntryProcessor result type
     */
    @SuppressWarnings("unchecked")
    private <R> Map<K, R> invokeAllUntilCommitted(final Filter filter, final TransactionId tid, 
            final EntryProcessor entryProcessor) {

        DistributedCacheService service = (DistributedCacheService) versionCache.getCacheService();
        PartitionSet remainingPartitions = new PartitionSet(service.getPartitionCount());
        remainingPartitions.fill();

        Map<K, VersionedKey<K>> retryMap = new HashMap<K, VersionedKey<K>>();
        Map<K, R> resultMap = new HashMap<K, R>();

        do {
            EntryProcessorInvoker<K, R> invoker = new EntryProcessorInvoker<K, R>(
                    cacheName, filter, tid, entryProcessor, remainingPartitions);

            Collection<EntryProcessorInvokerResult<K, R>> invocationResults =
                    invocationService.query(invoker, service.getOwnershipEnabledMembers()).values();

            for (EntryProcessorInvokerResult<K, R> result : invocationResults) {
                resultMap.putAll(result.getResultMap());
                retryMap.putAll(result.getRetryMap());
                remainingPartitions.remove(result.getPartitions());
            }
        } while (!remainingPartitions.isEmpty());

        for (Map.Entry<K, VersionedKey<K>> entry : retryMap.entrySet()) {
            waitForCommit(entry.getValue());
            while (true) {
                ProcessorResult<K, R> epr = (ProcessorResult<K, R>) keyCache.invoke(entry.getKey(), entryProcessor);
                if (epr == null) {
                    break;
                }
                if (!epr.isUncommitted()) {
                    resultMap.put(entry.getKey(), epr.getResult());
                    break;
                }
                waitForCommit(epr.getWaitKey());
            }
        }

        return resultMap;
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

        Map<K, VersionedKey<K>> retryMap = new HashMap<K, VersionedKey<K>>();
        Map<K, R> resultMap = new HashMap<K, R>();

        for (Map.Entry<K, ProcessorResult<K, R>> entry
                : ((Map<K, ProcessorResult<K, R>>) keyCache.invokeAll(keys, entryProcessor)).entrySet()) {
            if (entry.getValue().isUncommitted()) {
                retryMap.put(entry.getKey(), entry.getValue().getWaitKey());
            } else {
                resultMap.put(entry.getKey(), entry.getValue().getResult());
            }
        }

        for (Map.Entry<K, VersionedKey<K>> entry : retryMap.entrySet()) {
            waitForCommit(entry.getValue());
            while (true) {
                ProcessorResult<K, R> epr = (ProcessorResult<K, R>) keyCache.invoke(entry.getKey(), entryProcessor);
                if (epr == null) {
                    break;
                }
                if (!epr.isUncommitted()) {
                    resultMap.put(entry.getKey(), epr.getResult());
                    break;
                }
                waitForCommit(epr.getWaitKey());
            }
        }

        return resultMap;
    }

    @Override
    public <R> Map<K, R> invokeAll(final TransactionId tid, final IsolationLevel isolationLevel,
            final boolean autoCommit, final Collection<K> collKeys, final EntryProcessor agent) {
        EntryProcessor wrappedProcessor = new MVCCEntryProcessorWrapper<K, R>(
                tid, agent, isolationLevel, autoCommit, cacheName);
        return invokeAllUntilCommitted(collKeys, tid, wrappedProcessor);
    }

    @Override
    public <R> Map<K, R> invokeAll(final TransactionId tid, final IsolationLevel isolationLevel,
            final boolean autoCommit, final Filter filter, final EntryProcessor agent) {
        EntryProcessor wrappedProcessor = new MVCCEntryProcessorWrapper<K, R>(
                tid, agent, isolationLevel, autoCommit, cacheName, filter);
        return invokeAllUntilCommitted(filter, tid, wrappedProcessor);
    }

    @Override
    public void destroy() {
        keyCache.destroy();
        versionCache.destroy();
    }

    @Override
    public String getCacheName() {
        return cacheName.getLogicalName();
    }

    @Override
    public CacheService getCacheService() {
        return versionCache.getCacheService();
    }

    @Override
    public boolean isActive() {
        return versionCache.isActive();
    }

    @Override
    public void release() {
        keyCache.release();
        versionCache.release();
    }

    @Override
    public CacheName getMVCCCacheName() {
        return cacheName;
    }

}
