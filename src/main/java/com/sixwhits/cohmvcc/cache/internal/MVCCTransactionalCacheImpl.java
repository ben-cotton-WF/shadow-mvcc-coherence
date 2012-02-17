package com.sixwhits.cohmvcc.cache.internal;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.sixwhits.cohmvcc.cache.MVCCTransactionalCache;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.ProcessorResult;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.TransactionalValue;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.index.MVCCExtractor;
import com.sixwhits.cohmvcc.index.MVCCSurfaceFilter;
import com.sixwhits.cohmvcc.invocable.AggregatorWrapper;
import com.sixwhits.cohmvcc.invocable.MVCCEntryProcessorWrapper;
import com.sixwhits.cohmvcc.invocable.MVCCReadOnlyEntryProcessorWrapper;
import com.sixwhits.cohmvcc.invocable.ParallelAwareAggregatorWrapper;
import com.sixwhits.cohmvcc.transaction.internal.ReadMarkingProcessor;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.CacheService;
import com.tangosol.net.DistributedCacheService;
import com.tangosol.net.Member;
import com.tangosol.net.NamedCache;
import com.tangosol.net.partition.PartitionSet;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.EntryAggregator;
import com.tangosol.util.InvocableMap.EntryProcessor;
import com.tangosol.util.InvocableMap.ParallelAwareAggregator;
import com.tangosol.util.MapListener;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.aggregator.Count;
import com.tangosol.util.extractor.IdentityExtractor;
import com.tangosol.util.filter.PartitionedFilter;
import com.tangosol.util.processor.ExtractorProcessor;

public class MVCCTransactionalCacheImpl<K,V> implements MVCCTransactionalCache<K,V> {

	private final String cacheName;
	private final NamedCache keyCache;
	private final NamedCache versionCache;
	final String vcacheName;
	final String kcacheName;
	
	public MVCCTransactionalCacheImpl(String cacheName) {
		super();
		this.cacheName = cacheName;
		this.kcacheName = cacheName + "-keys";
		this.keyCache = CacheFactory.getCache(kcacheName);
		this.vcacheName = cacheName + "-versions";
		this.versionCache = CacheFactory.getCache(vcacheName);
		versionCache.addIndex(new MVCCExtractor(), false, null);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public V get(TransactionId tid, IsolationLevel isolationLevel, K key) {
		EntryProcessor ep = new MVCCReadOnlyEntryProcessorWrapper<K,V>(tid, new ExtractorProcessor(new IdentityExtractor()), isolationLevel, vcacheName);
		return (V) invokeUntilCommitted(key, tid, ep);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public V put(TransactionId tid, IsolationLevel isolationLevel, boolean autoCommit, K key, V value) {
		EntryProcessor ep = new MVCCEntryProcessorWrapper<K,V>(tid, new UnconditionalPutProcessor(value, true), isolationLevel, autoCommit, vcacheName);
		return invokeUntilCommitted(key, tid, ep);
	}

	@Override
	public void insert(TransactionId tid, IsolationLevel isolationLevel, boolean autoCommit, K key, V value) {
		EntryProcessor ep = new MVCCEntryProcessorWrapper<K,Object>(tid, new UnconditionalPutProcessor(value, false), isolationLevel, autoCommit, vcacheName);
		invokeUntilCommitted(key, tid, ep);
	}

	@SuppressWarnings("unchecked")
	@Override
	public V remove(TransactionId tid, IsolationLevel isolationLevel, boolean autoCommit, K key) {
		EntryProcessor ep = new MVCCEntryProcessorWrapper<K,V>(tid, new UnconditionalRemoveProcessor(), isolationLevel, autoCommit, vcacheName);
		return invokeUntilCommitted(key, tid, ep);
	}

	@Override
	public <R> R invoke(TransactionId tid, IsolationLevel isolationLevel, boolean autoCommit, K key, EntryProcessor agent) {
		EntryProcessor ep = new MVCCEntryProcessorWrapper<K, R>(tid, agent, isolationLevel, autoCommit, vcacheName);
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
			if (!v.isCommitted()) {
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
		return (Integer) filterAggregate(tid, isolationLevel, null, new Count());
	}

	@Override
	public boolean isEmpty(TransactionId tid, IsolationLevel isolationLevel) {
		return size(tid, isolationLevel) == 0;
	}

	@Override
	public boolean containsKey(TransactionId tid, IsolationLevel isolationLevel, Object key) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean containsValue(TransactionId tid, IsolationLevel isolationLevel, Object value) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void putAll(TransactionId tid, Map m) {
		// TODO Auto-generated method stub

	}

	@Override
	public void clear(TransactionId tid) {
		// TODO Auto-generated method stub

	}

	@Override
	public Set keySet(TransactionId tid, IsolationLevel isolationLevel) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection values(TransactionId tid, IsolationLevel isolationLevel) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set entrySet(TransactionId tid, IsolationLevel isolationLevel) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map getAll(TransactionId tid, IsolationLevel isolationLevel, Collection colKeys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void addIndex(ValueExtractor extractor, boolean fOrdered,
			Comparator comparator) {
		// TODO Auto-generated method stub

	}

	@SuppressWarnings("unchecked")
	@Override
	public Set<Map.Entry<K,V>> entrySet(TransactionId tid, IsolationLevel isolationLevel, Filter filter) {
		Filter surfaceFilter = new MVCCSurfaceFilter<K>(tid, filter);
		EntryProcessor ep = new MVCCReadOnlyEntryProcessorWrapper<K,V>(tid, new ExtractorProcessor(new IdentityExtractor()), isolationLevel, vcacheName);
		return ((Map<K,V>)invokeAllUntilCommitted(surfaceFilter, tid, ep)).entrySet();
	}

	@Override
	public Set entrySet(TransactionId tid, IsolationLevel isolationLevel, Filter filter, Comparator comparator) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set keySet(TransactionId tid, IsolationLevel isolationLevel, Filter filter) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeIndex(ValueExtractor extractor) {
		// TODO Auto-generated method stub

	}

	@Override
	public Object aggregate(TransactionId tid, IsolationLevel isolationLevel, Collection collKeys, EntryAggregator agent) {
		// TODO Auto-generated method stub
		return null;
	}

	private Object filterAggregate(TransactionId tid, IsolationLevel isolationLevel, Filter filter, EntryAggregator agent) {
		return aggregate(tid, isolationLevel, filter, agent);
	}
	
	@Override
	public Object aggregate(TransactionId tid, IsolationLevel isolationLevel, Filter filter, EntryAggregator agent) {
		Filter surfaceFilter = new MVCCSurfaceFilter<K>(tid, filter);
		if (isolationLevel != IsolationLevel.readUncommitted) {
			invokeAllUntilCommitted(surfaceFilter, tid, new ReadMarkingProcessor<K>(tid, isolationLevel, vcacheName));
		}
		EntryAggregator wrapper;
		if (agent instanceof ParallelAwareAggregator) {
			wrapper = new ParallelAwareAggregatorWrapper((ParallelAwareAggregator) agent);
		} else {
			wrapper = new AggregatorWrapper(agent);
		}
		return (Integer) versionCache.aggregate(surfaceFilter, wrapper);
	}

	@SuppressWarnings("unchecked")
	private <R> Map<K, R> invokeAllUntilCommitted(Filter filter, TransactionId tid,
			EntryProcessor entryProcessor) {
		
		DistributedCacheService service = (DistributedCacheService) versionCache.getCacheService();
		int partcount = service.getPartitionCount();
		PartitionSet partsProcessed = new PartitionSet(partcount);
		
		Map<K, VersionedKey<K>> retryMap = new HashMap<K, VersionedKey<K>>();
		Map<K, R> resultMap = new HashMap<K, R>();
		
		// TODO run in parallel
		for (Member member : (Set<Member>) service.getOwnershipEnabledMembers()) {
			PartitionSet memberParts = service.getOwnedPartitions(member);
			memberParts.remove(partsProcessed);
			Filter filterPart = new PartitionedFilter(filter, memberParts);
			Set<VersionedKey<K>> vkeys = versionCache.keySet(filterPart);
			Set<K> keys = new HashSet<K>();
			for (VersionedKey<K> vk: vkeys) {
				keys.add(vk.getNativeKey());
			}
			
			for (Map.Entry<K, ProcessorResult<K,R>> entry : ((Map<K, ProcessorResult<K,R>>)keyCache.invokeAll(keys, entryProcessor)).entrySet()) {
				if (entry.getValue().isUncommitted()) {
					retryMap.put(entry.getKey(), entry.getValue().getWaitKey());
				} else {
					resultMap.put(entry.getKey(), entry.getValue().getResult());
				}
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
	public Map invokeAll(TransactionId tid, IsolationLevel isolationLevel, Collection collKeys, EntryProcessor agent) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map invokeAll(TransactionId tid, IsolationLevel isolationLevel, Filter filter, EntryProcessor agent) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void destroy() {
		keyCache.destroy();
		versionCache.destroy();
	}

	@Override
	public String getCacheName() {
		return cacheName;
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
