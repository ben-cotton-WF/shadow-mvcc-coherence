package com.sixwhits.cohmvcc.invocable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.ProcessorResult;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.index.MVCCSurfaceFilter;
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

@Portable
public class EntryProcessorInvoker<K, R> implements Invocable {

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
	public static final int POF_EP = 3;
	@PortableProperty(POF_EP)
	private EntryProcessor entryProcessor;
	public static final int POF_PARTITIONS = 4;
	@PortableProperty(POF_PARTITIONS)
	private PartitionSet partitions = null;
	
	transient private PartitionSet memberParts;
	transient private Map<K, R> resultMap;
	transient private Map<K, VersionedKey<K>> retryMap;
	
	public EntryProcessorInvoker() {
		super();
	}

	public EntryProcessorInvoker(CacheName cacheName, Filter filter,
			TransactionId tid, EntryProcessor entryProcessor) {
		super();
		this.cacheName = cacheName;
		this.filter = filter;
		this.tid = tid;
		this.entryProcessor = entryProcessor;
	}

	public EntryProcessorInvoker(CacheName cacheName, Filter filter,
			TransactionId tid, EntryProcessor entryProcessor,
			PartitionSet partitions) {
		super();
		this.cacheName = cacheName;
		this.filter = filter;
		this.tid = tid;
		this.entryProcessor = entryProcessor;
		this.partitions = partitions;
	}

	@Override
	public void init(InvocationService invocationservice) {
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
		
		for (Map.Entry<K, ProcessorResult<K,R>> entry : ((Map<K, ProcessorResult<K,R>>)keyCache.invokeAll(keys, entryProcessor)).entrySet()) {
			if (entry.getValue().isUncommitted()) {
				retryMap.put(entry.getKey(), entry.getValue().getWaitKey());
			} else {
				resultMap.put(entry.getKey(), entry.getValue().getResult());
			}
		}
	}

	@Override
	public EntryProcessorInvokerResult<K, R> getResult() {
		return new EntryProcessorInvokerResult<K, R>(memberParts, resultMap, retryMap);
	}

}
