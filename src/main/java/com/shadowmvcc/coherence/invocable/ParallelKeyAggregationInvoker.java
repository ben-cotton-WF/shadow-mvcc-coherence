package com.shadowmvcc.coherence.invocable;

import static com.shadowmvcc.coherence.domain.IsolationLevel.readUncommitted;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.ProcessorResult;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.VersionedKey;
import com.shadowmvcc.coherence.transaction.internal.ReadMarkingProcessor;
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

/**
 * Invocable to perform local aggregation over a collection of keys on a member.
 * Affected entries will be marked as read (if appropriate for isolation level). 
 * The result contains the partial aggregation, a map of entries that couldn't be aggregated
 * because of uncommitted changes, and the set of partitions processed.
 * 
 * A set of partitions may be specified. If not, then the partitions present on the member at run time will be used.
 * 
 * Of the collection of keys given, only those present in this instance's partitions will be aggregated.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> the key type of the cache
 * @param <R> the result type of the invocation
 */
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

    private transient PartitionSet memberParts;
    private transient R aggregationResult;
    private transient Map<K, VersionedKey<K>> retryMap;

    /**
     * Default constructor for POF use only.
     */
    public ParallelKeyAggregationInvoker() {
        super();
    }

    /**
     * Constructor. No partitions so partitions local to the member will be used
     * @param cacheName name of cache to aggregate
     * @param keys keys to aggregate over
     * @param tid transaction id
     * @param aggregator the aggregator
     * @param isolationLevel isolation level
     */
    public ParallelKeyAggregationInvoker(final CacheName cacheName, final Collection<K> keys, 
            final TransactionId tid, final ParallelAwareAggregator aggregator, final IsolationLevel isolationLevel) {
        super();
        this.cacheName = cacheName;
        this.keys = keys;
        this.tid = tid;
        this.aggregator = aggregator;
        this.isolationLevel = isolationLevel;
    }

    /**
     * Constructor with partition set.
     * @param cacheName name of cache to aggregate
     * @param keys keys to aggregate over
     * @param tid transaction id
     * @param aggregator the aggregator
     * @param isolationLevel isolation level
     * @param partitions the partitions to include in the aggregation
     */
    public ParallelKeyAggregationInvoker(final CacheName cacheName, final Collection<K> keys, 
            final TransactionId tid, final ParallelAwareAggregator aggregator, final IsolationLevel isolationLevel, 
            final PartitionSet partitions) {
        super();
        this.cacheName = cacheName;
        this.keys = keys;
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

        KeyPartitioningStrategy kps = cacheService.getKeyPartitioningStrategy();
        Set<K> localKeys = new HashSet<K>();
        for (K key : keys) {
            int kp = kps.getKeyPartition(key);
            if (memberParts.contains(kp)) {
                localKeys.add(key);
            }
        }

        Map<K, ProcessorResult<K, VersionedKey<K>>> markMap
            = (Map<K, ProcessorResult<K, VersionedKey<K>>>) keyCache.invokeAll(
                localKeys, new ReadMarkingProcessor<K>(tid, isolationLevel, cacheName, true));

        Set<VersionedKey<K>> vkeys = new HashSet<VersionedKey<K>>();

        retryMap = new HashMap<K, VersionedKey<K>>();

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
        return new ParallelAggregationInvokerResult<K, R>(partitions, aggregationResult, retryMap);
    }

}
