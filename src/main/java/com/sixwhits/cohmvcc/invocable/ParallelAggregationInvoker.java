package com.sixwhits.cohmvcc.invocable;

import static com.sixwhits.cohmvcc.domain.IsolationLevel.readUncommitted;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.ProcessorResult;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.index.MVCCSurfaceFilter;
import com.sixwhits.cohmvcc.transaction.internal.ReadMarkingProcessor;
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
	
	transient private PartitionSet memberParts;
	transient private R aggregationResult;
	transient private Map<K, VersionedKey<K>> retryMap;
	
	public ParallelAggregationInvoker() {
		super();
	}

	public ParallelAggregationInvoker(CacheName cacheName, Filter filter,
			TransactionId tid, ParallelAwareAggregator aggregator, IsolationLevel isolationLevel) {
		super();
		this.cacheName = cacheName;
		this.filter = filter;
		this.tid = tid;
		this.aggregator = aggregator;
		this.isolationLevel = isolationLevel;
	}

	public ParallelAggregationInvoker(CacheName cacheName, Filter filter,
			TransactionId tid, ParallelAwareAggregator aggregator, IsolationLevel isolationLevel,
			PartitionSet partitions) {
		super();
		this.cacheName = cacheName;
		this.filter = filter;
		this.tid = tid;
		this.aggregator = aggregator;
		this.isolationLevel = isolationLevel;
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
		Set<VersionedKey<K>> candidateVkeys = versionCache.keySet(filterPart);
		Set<K> candidateKeys = new HashSet<K>();
		for (VersionedKey<K> vkey : candidateVkeys) {
			candidateKeys.add(vkey.getNativeKey());
		}
		Map<K, ProcessorResult<K,VersionedKey<K>>> markMap = (Map<K, ProcessorResult<K,VersionedKey<K>>>)keyCache.invokeAll(
				candidateKeys, new ReadMarkingProcessor<K>(tid, isolationLevel, cacheName, true));

		Set<VersionedKey<K>> vkeys = new HashSet<VersionedKey<K>>();
		
		retryMap = new HashMap<K, VersionedKey<K>>();
		
		for (Map.Entry<K, ProcessorResult<K,VersionedKey<K>>> entry : markMap.entrySet()) {
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
		return new ParallelAggregationInvokerResult<K,R>(partitions, aggregationResult, retryMap);
	}

}
