package com.sixwhits.cohmvcc.transaction.internal;

import static com.sixwhits.cohmvcc.domain.TransactionProcStatus.committing;
import static com.sixwhits.cohmvcc.domain.TransactionProcStatus.open;
import static com.sixwhits.cohmvcc.domain.TransactionProcStatus.rollingback;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionCacheValue;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.TransactionProcStatus;
import com.sixwhits.cohmvcc.invocable.InvocationObserverStatus;
import com.sixwhits.cohmvcc.transaction.TransactionException;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.Cluster;
import com.tangosol.net.Invocable;
import com.tangosol.net.InvocationObserver;
import com.tangosol.net.InvocationService;
import com.tangosol.net.Member;
import com.tangosol.net.NamedCache;
import com.tangosol.net.PartitionedService;
import com.tangosol.net.Service;
import com.tangosol.net.partition.PartitionSet;
import com.tangosol.util.Filter;
import com.tangosol.util.filter.NotFilter;
import com.tangosol.util.filter.PresentFilter;
import com.tangosol.util.processor.ConditionalPut;

/**
 * Cache service implementation for transaction operations in the grid. This class
 * relies on being a cluster member. Extend clients must call a separate invocation service
 * into a proxy node that can delegate to this.
 * 
 * TODO provide an extend implementation
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class TransactionCacheImpl implements TransactionCache {

    private final String invocationServiceName;
    private static final Filter NOTPRESENT = new NotFilter(PresentFilter.INSTANCE);
    
   /**
     * @param invocationServiceName name of the invocation service to use
     */
    public TransactionCacheImpl(final String invocationServiceName) {
        super();
        this.invocationServiceName = invocationServiceName;
    }
    
    /**
     * Static access wrapper to enable subclassing for unit testing.
     * @param cacheName cache name
     * @return the NamedCache
     */
    protected NamedCache getCache(final String cacheName) {
        return CacheFactory.getCache(cacheName);
    }
    
    /**
     * Static access wrapper to enable subclassing for unit testing.
     * @param serviceName the service name
     * @return the service
     */
    protected Service getService(final String serviceName) {
        return CacheFactory.getService(serviceName);
    }
    
    /**
     * Static access wrapper to enable subclassing for unit testing.
     * @return the cluster object
     */
    protected Cluster getCluster() {
        return CacheFactory.getCluster();
    }
    
    @Override
    public TransactionActualScope beginTransaction(final TransactionId transactionId,
            final IsolationLevel isolationLevel) {
        
        NamedCache transactionCache = getCache(CACHENAME);

        //TODO consider using cluster time here.
        TransactionCacheValue openTransaction = new TransactionCacheValue(open, new Date().getTime());
        
        if (transactionCache.invoke(transactionId, new ConditionalPut(NOTPRESENT, openTransaction, true)) != null) {
            throw new TransactionException("Transaction already exists: " + transactionId);
        }
        //TODO this is just echoing what was asked for. Implement the checks once windowing is implemented
        return new TransactionActualScope(isolationLevel, transactionId.getTimeStampMillis(), false);
        
    }

    @Override
    public void commitTransaction(final TransactionId transactionId, 
            final Map<CacheName, Set<Object>> cacheKeyMap, 
            final Map<CacheName, Set<Filter>> cacheFilterMap) {
        
        NamedCache transactionCache = getCache(CACHENAME);

        try {
            transactionCache.invoke(transactionId, TransactionStateUpdater.COMMIT);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof TransactionException) {
                throw (TransactionException) e.getCause();
            } else {
                throw e;
            }
        }

        actionTransaction(transactionId, cacheKeyMap, cacheFilterMap, committing);

        transactionCache.remove(transactionId);
    }

    @Override
    public void rollbackTransaction(final TransactionId transactionId, 
            final Map<CacheName, Set<Object>> cacheKeyMap, 
            final Map<CacheName, Set<Filter>> cacheFilterMap) {
        NamedCache transactionCache = getCache(CACHENAME);
        
        try {
            transactionCache.invoke(transactionId, TransactionStateUpdater.ROLLBACK);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof TransactionException) {
                throw (TransactionException) e.getCause();
            } else {
                throw e;
            }
        }

        actionTransaction(transactionId, cacheKeyMap, cacheFilterMap, rollingback);

        transactionCache.remove(transactionId);
    }
    
    /**
     * Perform the update or remove operations to effect the
     * commit or rollback on all the cache entries belonging to
     * this transaction.
     * @param transactionId the transaction id
     * @param cacheKeyMap map of affected caches to sets of keys
     * @param cacheFilterMap map of affected caches to sets of filters
     * @param transactionStatus commit or rollback
     */
    private void actionTransaction(final TransactionId transactionId, 
            final Map<CacheName, Set<Object>> cacheKeyMap, 
            final Map<CacheName, Set<Filter>> cacheFilterMap, 
            final TransactionProcStatus transactionStatus) {

        final Set<InvocationObserver> outstanding = new HashSet<InvocationObserver>(); 
        final BlockingQueue<InvocationObserverStatus> observerResultQueue = 
                new ArrayBlockingQueue<InvocationObserverStatus>(10);
        
        for (Map.Entry<CacheName, Set<Object>> cacheKeyEntry : cacheKeyMap.entrySet()) {
            invokeActionForKeyset(transactionId, cacheKeyEntry.getKey(), cacheKeyEntry.getValue(),
                    transactionStatus, outstanding, observerResultQueue);
        }
        
        for (Map.Entry<CacheName, Set<Filter>> cacheFilterEntry : cacheFilterMap.entrySet()) {
            PartitionedService cacheService =
                    (PartitionedService) getCache(
                            cacheFilterEntry.getKey().getVersionCacheName()).getCacheService();
            PartitionSet allPartitions = new PartitionSet(cacheService.getPartitionCount());
            allPartitions.fill();
            invokeActionForFilterSet(transactionId, allPartitions, cacheFilterEntry.getKey(),
                    cacheFilterEntry.getValue(), transactionStatus, outstanding, observerResultQueue);
        }
        
        while (!outstanding.isEmpty()) {
            InvocationObserverStatus observer;
            try {
                observer = observerResultQueue.take();
            } catch (InterruptedException e) {
                throw new TransactionException(e);
            }
            outstanding.remove(observer);
            if (observer.isFailed()) {
                if (observer instanceof KeyInvocationObserver) {
                    KeyInvocationObserver keyObserver = (KeyInvocationObserver) observer;
                    invokeActionForKeyset(transactionId, keyObserver.getCachename(), keyObserver.getKeys(),
                            transactionStatus, outstanding, observerResultQueue);
                } else if (observer instanceof FilterInvocationObserver) {
                    FilterInvocationObserver filterObserver = (FilterInvocationObserver) observer;
                    invokeActionForFilterSet(transactionId, filterObserver.getPartitionSet(),
                            filterObserver.getCachename(), filterObserver.getFilterSet(), transactionStatus,
                            outstanding, observerResultQueue);
                }
            }
        }
    }
    
    /**
     * Invoke the action for a set of filters on a cache. The partitions are split by
     * member and a separate invocable sent to each member.
     * @param transactionId transaction id
     * @param partitionSet partitions to process
     * @param cacheName cache name
     * @param filters filters
     * @param transactionStatus insert or rollback
     * @param outstanding each invocable send is added to this collection
     * @param observerResultQueue result queue for notifying completion of invocables
     */
    @SuppressWarnings("unchecked")
    private void invokeActionForFilterSet(
            final TransactionId transactionId,
            final PartitionSet partitionSet,
            final CacheName cacheName,
            final Set<Filter> filters,
            final TransactionProcStatus transactionStatus,
            final Set<InvocationObserver> outstanding,
            final BlockingQueue<InvocationObserverStatus> observerResultQueue) {
        
        InvocationService invocationService = (InvocationService) getService(invocationServiceName);
        PartitionedService cacheService =
                (PartitionedService) getCache(
                        cacheName.getVersionCacheName()).getCacheService();

        while (!partitionSet.isEmpty()) {
            for (Member member : (Set<Member>) getCluster().getMemberSet()) {
                PartitionSet memberPartitions = cacheService.getOwnedPartitions(member);
                memberPartitions.retain(partitionSet);
                if (!memberPartitions.isEmpty()) {
                    partitionSet.remove(memberPartitions);
                    Invocable invocable = new FilterTransactionInvocable(
                            transactionId, cacheName, filters, memberPartitions, transactionStatus);
                    FilterInvocationObserver observer = new FilterInvocationObserver(
                            memberPartitions, cacheName, filters, observerResultQueue);
                    outstanding.add(observer);
                    invocationService.execute(invocable, Collections.singleton(member), observer);
                }
            }            
        }
    }

    /**
     * Invoke the action for a set of keys on a cache. The keys are split by owning member
     * and a separate invocable sent to each member that owns any of the keys
     * @param transactionId transaction id
     * @param cacheName cach name
     * @param keyset set of keys to invoke on
     * @param transactionStatus insert or rollback
     * @param outstanding each invocable send is added to this collection
     * @param observerResultQueue result queue for notifying completion of invocables
     */
    private void invokeActionForKeyset(final TransactionId transactionId, final CacheName cacheName,
            final Set<Object> keyset, final TransactionProcStatus transactionStatus,
            final Set<InvocationObserver> outstanding,
            final BlockingQueue<InvocationObserverStatus> observerResultQueue) {
        
        InvocationService invocationService = (InvocationService) getService(invocationServiceName);

        PartitionedService cacheService =
                (PartitionedService) getCache(
                        cacheName.getVersionCacheName()).getCacheService();
        Map<Member, Set<Object>> memberKeyMap = new HashMap<Member, Set<Object>>();

        for (Object key : keyset) {
            Member member = cacheService.getKeyOwner(key); 
            if (!memberKeyMap.containsKey(member)) {
                memberKeyMap.put(member, new HashSet<Object>());
            }
            memberKeyMap.get(member).add(key);
        }
        
        for (Map.Entry<Member, Set<Object>> memberKeyEntry : memberKeyMap.entrySet()) {
            Invocable invocable = new KeyTransactionInvocable(
                    transactionId, cacheName, memberKeyEntry.getValue(), transactionStatus);
            KeyInvocationObserver observer = new KeyInvocationObserver(
                    memberKeyEntry.getValue(), cacheName, observerResultQueue);
            outstanding.add(observer);
            invocationService.execute(invocable, Collections.singleton(memberKeyEntry.getKey()), observer);
        }
    }
}
