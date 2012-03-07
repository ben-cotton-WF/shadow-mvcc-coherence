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
import com.sixwhits.cohmvcc.domain.TransactionalValue;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.event.MVCCEventTransformer;
import com.sixwhits.cohmvcc.event.MVCCMapListener;
import com.sixwhits.cohmvcc.index.FilterWrapper;
import com.sixwhits.cohmvcc.index.MVCCExtractor;
import com.sixwhits.cohmvcc.index.MVCCSurfaceFilter;
import com.sixwhits.cohmvcc.invocable.AggregatorWrapper;
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
import com.tangosol.util.MapListener;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.aggregator.Count;
import com.tangosol.util.extractor.IdentityExtractor;
import com.tangosol.util.filter.AlwaysFilter;
import com.tangosol.util.filter.EqualsFilter;
import com.tangosol.util.filter.MapEventTransformerFilter;
import com.tangosol.util.processor.ExtractorProcessor;

public class MVCCTransactionalCacheImpl<K,V> implements MVCCTransactionalCache<K,V> {

	private final NamedCache keyCache;
	private final NamedCache versionCache;
	final CacheName cacheName;
	private final InvocationService invocationService;
	//TODO: determine when it is safe to remove entries from this map
	private final ConcurrentMap<MapListener, MVCCMapListener<K, V>> listenerMap = new ConcurrentHashMap<MapListener, MVCCMapListener<K,V>>();
	
	public MVCCTransactionalCacheImpl(String cacheName) {
		super();
		this.cacheName = new CacheName(cacheName);
		this.keyCache = CacheFactory.getCache(this.cacheName.getKeyCacheName());
		this.versionCache = CacheFactory.getCache(this.cacheName.getVersionCacheName());
		versionCache.addIndex(new MVCCExtractor(), false, null);
		this.invocationService = (InvocationService) CacheFactory.getService("InvocationService");
	}
	
	@Override
	public V get(TransactionId tid, IsolationLevel isolationLevel, K key) {
		EntryProcessor ep = new MVCCReadOnlyEntryProcessorWrapper<K,V>(tid, new ExtractorProcessor(new IdentityExtractor()), isolationLevel, cacheName);
		return invokeUntilCommitted(key, tid, ep);
	}
	
	@Override
	public V put(TransactionId tid, IsolationLevel isolationLevel, boolean autoCommit, K key, V value) {
		EntryProcessor ep = new MVCCEntryProcessorWrapper<K,V>(tid, new UnconditionalPutProcessor(value, true), isolationLevel, autoCommit, cacheName);
		return invokeUntilCommitted(key, tid, ep);
	}

	@Override
	public void insert(TransactionId tid, boolean autoCommit, K key, V value) {
		EntryProcessor ep = new MVCCEntryProcessorWrapper<K,Object>(tid, new UnconditionalPutProcessor(value, false), readProhibited, autoCommit, cacheName);
		invokeUntilCommitted(key, tid, ep);
	}

	@Override
	public V remove(TransactionId tid, IsolationLevel isolationLevel, boolean autoCommit, K key) {
		EntryProcessor ep = new MVCCEntryProcessorWrapper<K,V>(tid, new UnconditionalRemoveProcessor(), isolationLevel, autoCommit, cacheName);
		return invokeUntilCommitted(key, tid, ep);
	}

	@Override
	public <R> R invoke(TransactionId tid, IsolationLevel isolationLevel, boolean autoCommit, K key, EntryProcessor agent) {
		EntryProcessor ep = new MVCCEntryProcessorWrapper<K, R>(tid, agent, isolationLevel, autoCommit, cacheName);
		return invokeUntilCommitted(key, tid, ep);
	}

	private <R> R invokeUntilCommitted(K key, TransactionId tid, EntryProcessor ep) {
		while (true) {
			@SuppressWarnings("unchecked")
			ProcessorResult<K,R> epr = (ProcessorResult<K,R>) keyCache.invoke(key, ep);
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
	
	private void waitForCommit(VersionedKey<K> awaitedKey) {
		VersionCommitListener vcl = new VersionCommitListener();
		try {
			versionCache.addMapListener(vcl, awaitedKey, false);
			TransactionalValue v = (TransactionalValue) versionCache.get(awaitedKey);
			if (v != null && !v.isCommitted()) {
				vcl.waitForCommit();
			}
			return;
		} finally {
			versionCache.removeMapListener(vcl);
		}
	}
		
	@Override
	public void addMapListener(MapListener listener, TransactionId tid, IsolationLevel isolationLevel) {
		MVCCMapListener<K,V> mvccml = new MVCCMapListener<K, V>(listener, versionCache.getCacheService().getSerializer());
		listenerMap.putIfAbsent(listener, mvccml);
		versionCache.addMapListener(mvccml,
				new MapEventTransformerFilter(AlwaysFilter.INSTANCE, new MVCCEventTransformer<Integer>(isolationLevel, tid, cacheName)), false);
		versionCache.addMapListener(mvccml);
	}

	@Override
	public void addMapListener(MapListener listener, TransactionId tid, IsolationLevel isolationLevel, Object oKey, boolean fLite) {
		Filter keyFilter = new EqualsFilter(KEYEXTRACTOR, oKey);
		MVCCMapListener<K,V> mvccml = new MVCCMapListener<K, V>(listener, versionCache.getCacheService().getSerializer());
		listenerMap.putIfAbsent(listener, mvccml);
		versionCache.addMapListener(mvccml,
				new MapEventTransformerFilter(keyFilter, new MVCCEventTransformer<Integer>(isolationLevel, tid, cacheName)), false);
		versionCache.addMapListener(mvccml);
	}

	@Override
	public void addMapListener(MapListener listener, TransactionId tid, IsolationLevel isolationLevel, Filter filter,
			boolean fLite) {
		MVCCMapListener<K,V> mvccml = new MVCCMapListener<K, V>(listener, versionCache.getCacheService().getSerializer());
		listenerMap.putIfAbsent(listener, mvccml);
		versionCache.addMapListener(mvccml,
				new MapEventTransformerFilter(new FilterWrapper(filter), new MVCCEventTransformer<Integer>(isolationLevel, tid, cacheName)), false);
		versionCache.addMapListener(mvccml);
	}

	@Override
	public void removeMapListener(MapListener listener) {
		versionCache.removeMapListener(listenerMap.get(listener));
	}

	@Override
	public void removeMapListener(MapListener listener, Object oKey) {
		versionCache.removeMapListener(listenerMap.get(listener), oKey);
	}

	@Override
	public void removeMapListener(MapListener listener, Filter filter) {
		versionCache.removeMapListener(listenerMap.get(listener), filter);
	}

	@Override
	public int size(TransactionId tid, IsolationLevel isolationLevel) {
		return aggregate(tid, isolationLevel, (Filter)null, new Count());
	}

	@Override
	public boolean isEmpty(TransactionId tid, IsolationLevel isolationLevel) {
		return size(tid, isolationLevel) == 0;
	}

	@Override
	public boolean containsKey(TransactionId tid, IsolationLevel isolationLevel, K key) {
		EntryProcessor ep = new MVCCReadOnlyEntryProcessorWrapper<K,Boolean>(tid, new ExistenceCheckProcessor(), isolationLevel, cacheName);
		Boolean result = invokeUntilCommitted(key, tid, ep);
		return result == null ? false : result;
	}

	@Override
	public boolean containsValue(TransactionId tid, IsolationLevel isolationLevel, V value) {
		EntryProcessor ep = new MVCCReadOnlyEntryProcessorWrapper<K,Boolean>(tid, new ExistenceCheckProcessor(), isolationLevel, cacheName);
		Filter filter = new EqualsFilter(IdentityExtractor.INSTANCE, value);
		// TODO optimise - we only need to find a single committed matching entry, even if others are uncommitted
		Map<K,Boolean> result = invokeAllUntilCommitted(filter, tid, ep);
		for (Boolean exists : result.values()) {
			if (exists != null && exists) {
				return true;
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void putAll(TransactionId tid, boolean autoCommit, Map<K, V> m) {
		DistributedCacheService service = (DistributedCacheService) versionCache.getCacheService();
		KeyPartitioningStrategy partitionStrategy = service.getKeyPartitioningStrategy();
		
		Map<Integer, Map<K, V>> memberValueMap = new HashMap<Integer, Map<K,V>>();
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
			EntryProcessor wrappedProcessor = new MVCCEntryProcessorWrapper<K, Object>(tid, putProcessor, readProhibited, autoCommit, cacheName);
			invokeAllUntilCommitted(valueMap.keySet(), tid, wrappedProcessor);
		}

	}

	@Override
	public void clear(TransactionId tid, boolean autoCommit) {
		EntryProcessor ep = new MVCCEntryProcessorWrapper<K,V>(tid, new UnconditionalRemoveProcessor(false), readProhibited, autoCommit, cacheName);
		invokeAllUntilCommitted((Filter)null, tid, ep);
	}

	@Override
	public Set<K> keySet(TransactionId tid, IsolationLevel isolationLevel) {
		//TODO Wrap to add java.util.Map semantics to update underlying cache
		return keySet(tid, isolationLevel, null);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Collection<V> values(TransactionId tid, IsolationLevel isolationLevel) {
		//TODO find a more efficient implementation that doesn't require the complete map to be returned
		EntryProcessor ep = new MVCCReadOnlyEntryProcessorWrapper<K,V>(tid, new ExtractorProcessor(new IdentityExtractor()), isolationLevel, cacheName, null);
		return ((Map<K,V>)invokeAllUntilCommitted((Filter)null, tid, ep)).values();
	}

	@Override
	public Set<Map.Entry<K, V>> entrySet(TransactionId tid, IsolationLevel isolationLevel) {
		//TODO Wrap to add java.util.Map semantics to update underlying cache
		return entrySet(tid, isolationLevel, null);
	}

	@Override
	public Map<K,V> getAll(TransactionId tid, IsolationLevel isolationLevel, Collection<K> colKeys) {
		EntryProcessor ep = new MVCCReadOnlyEntryProcessorWrapper<K,V>(tid, new ExtractorProcessor(new IdentityExtractor()), isolationLevel, cacheName);
		return invokeAllUntilCommitted(colKeys, tid, ep);
	}

	@Override
	public void addIndex(ValueExtractor extractor, boolean fOrdered,
			Comparator<V> comparator) {
		// TODO Auto-generated method stub

	}

	@SuppressWarnings("unchecked")
	@Override
	public Set<Map.Entry<K,V>> entrySet(TransactionId tid, IsolationLevel isolationLevel, Filter filter) {
		EntryProcessor ep = new MVCCReadOnlyEntryProcessorWrapper<K,V>(tid, new ExtractorProcessor(new IdentityExtractor()), isolationLevel, cacheName, filter);
		return ((Map<K,V>)invokeAllUntilCommitted(filter, tid, ep)).entrySet();
	}

	@Override
	public Set<Map.Entry<K,V>> entrySet(TransactionId tid, IsolationLevel isolationLevel, Filter filter, Comparator<V> comparator) {
		// TODO Auto-generated method stub
		// TODO is the comparator on the value or the entry?
		return null;
	}

	@Override
	public Set<K> keySet(TransactionId tid, IsolationLevel isolationLevel, Filter filter) {
		EntryProcessor ep = new MVCCReadOnlyEntryProcessorWrapper<K,Object>(tid, null, isolationLevel, cacheName, filter);
		return ((Map<K,Object>)invokeAllUntilCommitted(filter, tid, ep)).keySet();
	}

	@Override
	public void removeIndex(ValueExtractor extractor) {
		// TODO Auto-generated method stub

	}

	@Override
	public <R> R aggregate(TransactionId tid, IsolationLevel isolationLevel, Collection<K> collKeys, EntryAggregator agent) {
		
		if (agent instanceof ParallelAwareAggregator) {
			ParallelAwareAggregatorWrapper wrapper = new ParallelAwareAggregatorWrapper((ParallelAwareAggregator)agent, cacheName);
			return aggregateParallel(tid, isolationLevel, collKeys, wrapper);
		} else {
			AggregatorWrapper wrapper = new AggregatorWrapper(agent, cacheName);
			return aggregateSerial(tid, isolationLevel, collKeys, wrapper);
		}
	}

	@Override
	public <R> R aggregate(TransactionId tid, IsolationLevel isolationLevel, Filter filter, EntryAggregator agent) {
		if (agent instanceof ParallelAwareAggregator) {
			ParallelAwareAggregatorWrapper wrapper = new ParallelAwareAggregatorWrapper((ParallelAwareAggregator)agent, cacheName);
			return aggregateParallel(tid, isolationLevel, filter, wrapper);
		} else {
			AggregatorWrapper wrapper = new AggregatorWrapper(agent, cacheName);
			return aggregateSerial(tid, isolationLevel, filter, wrapper);
		}
	}

	@SuppressWarnings("unchecked")
	private <R> R aggregateSerial(TransactionId tid, IsolationLevel isolationLevel, Filter filter, EntryAggregator agent) {
		if (isolationLevel == repeatableRead || isolationLevel == serializable) {
			invokeAllUntilCommitted(filter, tid, new ReadMarkingProcessor<K>(tid, isolationLevel, cacheName));
		}
		EntryAggregator wrapper;
		wrapper = new AggregatorWrapper(agent, cacheName);
		// TODO restrict filter to include only items already marked read with this tid for repeatableRead
		MVCCSurfaceFilter<K> surfaceFilter = new MVCCSurfaceFilter<K>(tid, filter);
		return (R) versionCache.aggregate(surfaceFilter, wrapper);
	}
	
	@SuppressWarnings("unchecked")
	private <R> R aggregateSerial(TransactionId tid, IsolationLevel isolationLevel, Collection<K> keys, EntryAggregator agent) {
		ReadMarkingProcessor<K> readMarker = new ReadMarkingProcessor<K>(tid, isolationLevel, cacheName, true);
		Map<K, ProcessorResult<K,VersionedKey<K>>> markMap = (Map<K, ProcessorResult<K,VersionedKey<K>>>)keyCache.invokeAll(
					keys, readMarker);
		EntryAggregator wrapper;
		Set<VersionedKey<K>> vkeys = new HashSet<VersionedKey<K>>(markMap.size());
		for (Map.Entry<K, ProcessorResult<K,VersionedKey<K>>> entry : markMap.entrySet()) {
			ProcessorResult<K, VersionedKey<K>> pr = entry.getValue();
			while (isolationLevel != readUncommitted && pr != null && pr.isUncommitted()) {
				waitForCommit(pr.getWaitKey());
				pr = (ProcessorResult<K, VersionedKey<K>>) keyCache.invoke(entry.getKey(), readMarker);
			}
			if (pr != null) {
				vkeys.add(pr.getResult());
			}
			
		}
		wrapper = new AggregatorWrapper(agent, cacheName);
		return (R) versionCache.aggregate(vkeys, wrapper);
	}
	
	@SuppressWarnings("unchecked")
	private <R> R aggregateParallel(TransactionId tid, IsolationLevel isolationLevel, Filter filter, ParallelAwareAggregator agent) {
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

				for (Map.Entry<K, ProcessorResult<K,VersionedKey<K>>> entry :
					((Map<K, ProcessorResult<K,VersionedKey<K>>>)keyCache.invokeAll(retryKeys, new FilterValidateEntryProcessor<K>(tid, isolationLevel, cacheName, filter))).entrySet()) {
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
	
	@SuppressWarnings("unchecked")
	private <R> R aggregateParallel(TransactionId tid, IsolationLevel isolationLevel, Collection<K> keys, ParallelAwareAggregator agent) {
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
					ReadMarkingProcessor<K> readMarker = new ReadMarkingProcessor<K>(tid, isolationLevel, cacheName, true);
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

	@SuppressWarnings("unchecked")
	private <R> Map<K, R> invokeAllUntilCommitted(Filter filter, TransactionId tid,
			EntryProcessor entryProcessor) {
		
		DistributedCacheService service = (DistributedCacheService) versionCache.getCacheService();
		PartitionSet remainingPartitions = new PartitionSet(service.getPartitionCount());
		remainingPartitions.fill();
		
		Map<K, VersionedKey<K>> retryMap = new HashMap<K, VersionedKey<K>>();
		Map<K, R> resultMap = new HashMap<K, R>();

		do {
			EntryProcessorInvoker<K, R> invoker = new EntryProcessorInvoker<K, R>(cacheName, filter, tid, entryProcessor, remainingPartitions);

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
				ProcessorResult<K,R> epr = (ProcessorResult<K,R>) keyCache.invoke(entry.getKey(), entryProcessor);
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
	
	@SuppressWarnings("unchecked")
	private <R> Map<K, R> invokeAllUntilCommitted(Collection<K> keys, TransactionId tid,
			EntryProcessor entryProcessor) {
		
		Map<K, VersionedKey<K>> retryMap = new HashMap<K, VersionedKey<K>>();
		Map<K, R> resultMap = new HashMap<K, R>();
		
		for (Map.Entry<K, ProcessorResult<K,R>> entry : ((Map<K, ProcessorResult<K,R>>)keyCache.invokeAll(keys, entryProcessor)).entrySet()) {
			if (entry.getValue().isUncommitted()) {
				retryMap.put(entry.getKey(), entry.getValue().getWaitKey());
			} else {
				resultMap.put(entry.getKey(), entry.getValue().getResult());
			}
		}
		
		for (Map.Entry<K, VersionedKey<K>> entry : retryMap.entrySet()) {
			waitForCommit(entry.getValue());
			while (true) {
				ProcessorResult<K,R> epr = (ProcessorResult<K,R>) keyCache.invoke(entry.getKey(), entryProcessor);
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
	public <R> Map<K, R> invokeAll(TransactionId tid, IsolationLevel isolationLevel, boolean autoCommit, Collection<K> collKeys, EntryProcessor agent) {
		EntryProcessor wrappedProcessor = new MVCCEntryProcessorWrapper<K, R>(tid, agent, isolationLevel, autoCommit, cacheName);
		return invokeAllUntilCommitted(collKeys, tid, wrappedProcessor);
	}

	@Override
	public <R> Map<K, R> invokeAll(TransactionId tid, IsolationLevel isolationLevel, boolean autoCommit, Filter filter, EntryProcessor agent) {
		EntryProcessor wrappedProcessor = new MVCCEntryProcessorWrapper<K, R>(tid, agent, isolationLevel, autoCommit, cacheName, filter);
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

}
