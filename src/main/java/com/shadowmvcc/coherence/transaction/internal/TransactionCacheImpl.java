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

package com.shadowmvcc.coherence.transaction.internal;

import static com.shadowmvcc.coherence.domain.TransactionProcStatus.committing;
import static com.shadowmvcc.coherence.domain.TransactionProcStatus.open;
import static com.shadowmvcc.coherence.domain.TransactionProcStatus.rollingback;

import java.util.Map;
import java.util.Set;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.config.ClusterTimeProviderFactory;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.TransactionCacheValue;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.TransactionProcStatus;
import com.shadowmvcc.coherence.invocable.InvocationServiceHelper;
import com.shadowmvcc.coherence.invocable.InvocationServiceHelper.InvocableFactory;
import com.shadowmvcc.coherence.transaction.TransactionException;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.Cluster;
import com.tangosol.net.Invocable;
import com.tangosol.net.NamedCache;
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
    public void beginTransaction(final TransactionId transactionId,
            final IsolationLevel isolationLevel, final TransactionExpiryListener expiryListener) {
        
        NamedCache transactionCache = getCache(CACHENAME);
        transactionCache.addMapListener(expiryListener, transactionId, false);

        long now = ClusterTimeProviderFactory.getInstance().getClusterTime();
        
        TransactionCacheValue openTransaction = new TransactionCacheValue(open, now);
        
        if (transactionCache.invoke(transactionId, new ConditionalPut(NOTPRESENT, openTransaction, true)) != null) {
            throw new TransactionException("Transaction already exists: " + transactionId);
        }
        
    }

    @Override
    public void commitTransaction(final TransactionId transactionId, 
            final Map<CacheName, Set<Object>> cacheKeyMap, 
            final Map<CacheName, PartitionSet> cachePartitionMap) {
        
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

        actionTransaction(transactionId, cacheKeyMap, cachePartitionMap, committing);

        transactionCache.remove(transactionId);
    }

    @Override
    public void rollbackTransaction(final TransactionId transactionId, 
            final Map<CacheName, Set<Object>> cacheKeyMap, 
            final Map<CacheName, PartitionSet> cachePartitionMap) {
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

        actionTransaction(transactionId, cacheKeyMap, cachePartitionMap, rollingback);

        transactionCache.remove(transactionId);
    }
    
    /**
     * Perform the update or remove operations to effect the
     * commit or rollback on all the cache entries belonging to
     * this transaction.
     * @param transactionId the transaction id
     * @param cacheKeyMap map of affected caches to sets of keys
     * @param cachePartitionMap map of affected caches to sets of partitions
     * @param transactionStatus commit or rollback
     */
    private void actionTransaction(final TransactionId transactionId, 
            final Map<CacheName, Set<Object>> cacheKeyMap, 
            final Map<CacheName, PartitionSet> cachePartitionMap, 
            final TransactionProcStatus transactionStatus) {

        InvocationServiceHelper<Object> invocationHelper = new InvocationServiceHelper<Object>(invocationServiceName);
        
        for (Map.Entry<CacheName, Set<Object>> cacheKeyEntry : cacheKeyMap.entrySet()) {
            
            final CacheName cacheName = cacheKeyEntry.getKey();
            InvocableFactory<Set<Object>> keyInvocableFactory = new InvocableFactory<Set<Object>>() {
                @Override
                public Invocable getInvocable(final Set<Object> invocationTargetSet) {
                    return new KeyTransactionInvocable(
                            transactionId, cacheName, invocationTargetSet, transactionStatus);
                }
            };
            
            invocationHelper.invokeActionForKeyset(cacheName, cacheKeyEntry.getValue(),
                    keyInvocableFactory);
            
        }
        
        for (Map.Entry<CacheName, PartitionSet> cachePartitionEntry : cachePartitionMap.entrySet()) {
            
            final CacheName cacheName = cachePartitionEntry.getKey();
            
            InvocableFactory<PartitionSet> partitionInvocableFactory = new InvocableFactory<PartitionSet>() {

                @Override
                public Invocable getInvocable(final PartitionSet invocationTargetSet) {
                    return new PartitionTransactionInvocable(
                            transactionId, cacheName, invocationTargetSet, transactionStatus);
                }
                
            };
            
            invocationHelper.invokeActionForPartitionSet(
                    cachePartitionEntry.getValue(), cacheName, partitionInvocableFactory);
        }
        
        try {
            invocationHelper.waitForAllInvocations();
        } catch (Throwable t) {
            throw new TransactionException(t);
        }
        
    }

    @Override
    public void unregisterExpiryListener(final TransactionId transactionId,
            final TransactionExpiryListener expiryListener) {
        NamedCache transactionCache = getCache(CACHENAME);
        transactionCache.removeMapListener(expiryListener, transactionId);
    }

    @Override
    public void registerExpiryListener(final TransactionId transactionId,
            final TransactionExpiryListener expiryListener) {
        NamedCache transactionCache = getCache(CACHENAME);
        transactionCache.addMapListener(expiryListener, transactionId, false);
    }
    
}
