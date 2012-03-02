package com.sixwhits.cohmvcc.invocable;

import static com.sixwhits.cohmvcc.domain.IsolationLevel.readUncommitted;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.ProcessorResult;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.transaction.internal.ReadMarkingProcessor;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.DistributedCacheService;
import com.tangosol.net.Invocable;
import com.tangosol.net.InvocationService;
import com.tangosol.net.Member;
import com.tangosol.net.NamedCache;
import com.tangosol.net.partition.KeyPartitioningStrategy;
import com.tangosol.net.partition.PartitionSet;
import com.tangosol.util.InvocableMap.ParallelAwareAggregator;

@Portable
public class ParallelKeyAggregationInvoker<K, R> implements Invocable {

	private static final long serialVersionUID = 4499295487465987875L;

	public static final int POF_CACHENAME = 0;
	@PortableProperty(POF_CACHENAME)
	private CacheName cacheName;
	public static final int POF_KEYS = 1;
	@PortableProperty(POF_KEYS)
	private Collection<K> keys;
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
	
	public ParallelKeyAggregationInvoker() {
		super();
	}

	public ParallelKeyAggregationInvoker(CacheName cacheName, Collection<K> keys,
			TransactionId tid, ParallelAwareAggregator aggregator, IsolationLevel isolationLevel) {
		super();
		this.cacheName = cacheName;
		this.keys = keys;
		this.tid = tid;
		this.aggregator = aggregator;
		this.isolationLevel = isolationLevel;
	}

	public ParallelKeyAggregationInvoker(CacheName cacheName, Collection<K> keys,
			TransactionId tid, ParallelAwareAggregator aggregator, IsolationLevel isolationLevel,
			PartitionSet partitions) {
		super();
		this.cacheName = cacheName;
		this.keys = keys;
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
		
		KeyPartitioningStrategy kps = cacheService.getKeyPartitioningStrategy();
		Set<K> localKeys = new HashSet<K>(); 
		for (K key : keys) {
			int kp = kps.getKeyPartition(key);
			if (memberParts.contains(kp)) {
				localKeys.add(key);
			}
		}
		
		Map<K, ProcessorResult<K,VersionedKey<K>>> markMap = (Map<K, ProcessorResult<K,VersionedKey<K>>>)keyCache.invokeAll(
				localKeys, new ReadMarkingProcessor<K>(tid, isolationLevel, cacheName, true));

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
