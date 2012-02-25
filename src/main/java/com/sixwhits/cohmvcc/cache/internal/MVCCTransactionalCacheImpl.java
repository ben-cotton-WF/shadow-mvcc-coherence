package com.sixwhits.cohmvcc.cache.internal;

import static com.sixwhits.cohmvcc.domain.IsolationLevel.readProhibited;
import static com.sixwhits.cohmvcc.domain.IsolationLevel.repeatableRead;
import static com.sixwhits.cohmvcc.domain.IsolationLevel.serializable;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.cache.MVCCTransactionalCache;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.ProcessorResult;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.TransactionalValue;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.index.MVCCExtractor;
import com.sixwhits.cohmvcc.index.MVCCSurfaceFilter;
import com.sixwhits.cohmvcc.invocable.AggregatorWrapper;
import com.sixwhits.cohmvcc.invocable.EntryProcessorInvoker;
import com.sixwhits.cohmvcc.invocable.EntryProcessorInvokerResult;
import com.sixwhits.cohmvcc.invocable.MVCCEntryProcessorWrapper;
import com.sixwhits.cohmvcc.invocable.MVCCReadOnlyEntryProcessorWrapper;
import com.sixwhits.cohmvcc.invocable.ParallelAwareAggregatorWrapper;
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
import com.tangosol.util.filter.EqualsFilter;
import com.tangosol.util.processor.ExtractorProcessor;

public class MVCCTransactionalCacheImpl<K,V> implements MVCCTransactionalCache<K,V> {

	private final NamedCache keyCache;
	private final NamedCache versionCache;
	final CacheName cacheName;
	private final InvocationService invocationService;
	
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
	public void addMapListener(MapListener listener, TransactionId tid) {
		// TODO Auto-generated method stub

	}

	@Override
	public void addMapListener(MapListener listener, TransactionId tid, Object oKey, boolean fLite) {
		// TODO Auto-generated method stub

	}

	@Override
	public void addMapListener(MapListener listener, TransactionId tid, Filter filter,
			boolean fLite) {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeMapListener(MapListener listener, TransactionId tid) {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeMapListener(MapListener listener, TransactionId tid, Object oKey) {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeMapListener(MapListener listener, TransactionId tid, Filter filter) {
		// TODO Auto-generated method stub

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
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R> R aggregate(TransactionId tid, IsolationLevel isolationLevel, Filter filter, EntryAggregator agent) {
		if (isolationLevel == repeatableRead || isolationLevel == serializable) {
			invokeAllUntilCommitted(filter, tid, new ReadMarkingProcessor<K>(tid, isolationLevel, cacheName));
		}
		EntryAggregator wrapper;
		if (agent instanceof ParallelAwareAggregator) {
			wrapper = new ParallelAwareAggregatorWrapper((ParallelAwareAggregator) agent);
		} else {
			wrapper = new AggregatorWrapper(agent);
		}
		MVCCSurfaceFilter<K> surfaceFilter = new MVCCSurfaceFilter<K>(tid, filter);
		return (R) versionCache.aggregate(surfaceFilter, wrapper);
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
		EntryProcessor wrappedProcessor = new MVCCEntryProcessorWrapper<K, R>(tid, agent, isolationLevel, autoCommit, cacheName);
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
