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

import static com.shadowmvcc.coherence.domain.TransactionStatus.committed;
import static com.shadowmvcc.coherence.domain.TransactionStatus.open;
import static com.shadowmvcc.coherence.domain.TransactionStatus.rolledback;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.TransactionStatus;
import com.shadowmvcc.coherence.transaction.Transaction;
import com.shadowmvcc.coherence.transaction.TransactionException;
import com.shadowmvcc.coherence.transaction.TransactionNotificationListener;
import com.tangosol.net.partition.PartitionSet;

/**
 * Implementation of {@link Transaction}. Stores all open state
 * including collections of keys and filters affected by the transaction.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class TransactionImpl implements Transaction {

    private final TransactionId transactionId;
    private final IsolationLevel isolationLevel;
    private final TransactionNotificationListener notificationListener;
    private final TransactionCache transactionCache;
    private volatile boolean rollbackOnly = false;
    private volatile TransactionStatus transactionStatus = open;

    private Map<CacheName, Set<Object>> cacheKeyMap = new HashMap<CacheName, Set<Object>>();
    private Map<CacheName, PartitionSet> cachePartitionMap = new HashMap<CacheName, PartitionSet>();

    /**
     * Constructor.
     * @param transactionId transaction id
     * @param isolationLevel isolation level
     * @param notificationListener listener to notify on commit or rollback
     * @param transactionCache transaction cache DAO
     */
    public TransactionImpl(final TransactionId transactionId, final IsolationLevel isolationLevel, 
            final TransactionNotificationListener notificationListener, final TransactionCache transactionCache) {
        super();
        this.notificationListener = notificationListener;
        this.transactionCache = transactionCache;
        this.transactionId = transactionId;
        this.isolationLevel = getIsolationLevel();
        transactionCache.beginTransaction(transactionId, isolationLevel);
    }

    @Override
    public TransactionId getTransactionId() {
        return transactionId;
    }

    @Override
    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    @Override
    public boolean isAutoCommit() {
        return false;
    }

    /**
     * Add a key for a cache to the set of affected entries.
     * @param cacheName the cache name
     * @param key the logical key
     */
    private void addCacheKey(final CacheName cacheName, final Object key) {
        synchronized (cacheKeyMap) {
            if (!cacheKeyMap.containsKey(cacheName)) {
                cacheKeyMap.put(cacheName, new HashSet<Object>());
            }
            cacheKeyMap.get(cacheName).add(key);
        }
    }

    /**
     * Add a collection of keys for a cache to the set of affected entries.
     * @param cacheName the cache name
     * @param keys the logical keys
     */
    private void addCacheKeys(final CacheName cacheName, final Collection<Object> keys) {
        synchronized (cacheKeyMap) {
            if (!cacheKeyMap.containsKey(cacheName)) {
                cacheKeyMap.put(cacheName, new HashSet<Object>());
            }
            cacheKeyMap.get(cacheName).addAll(keys);
        }
    }

    @Override
    public void addKeyAffected(final CacheName cacheName, final Object key) {
        addCacheKey(cacheName, key);
    }

    @Override
    public void addKeySetAffected(final CacheName cacheName, final Collection<Object> keys) {
        addCacheKeys(cacheName, keys);
    }
    
    @Override
    public void addPartitionSetAffected(final CacheName cacheName,
            final PartitionSet addSet) {
        synchronized (cachePartitionMap) {
            if (!cachePartitionMap.containsKey(cacheName)) {
                cachePartitionMap.put(cacheName, new PartitionSet(addSet));
            } else {
                cachePartitionMap.get(cacheName).add(addSet);
            }
        }
    }

    @Override
    public void setRollbackOnly() {
        rollbackOnly = true;
    }

    @Override
    public void commit() {
        if (rollbackOnly) {
            throw new TransactionException("Transaction is in rollback only mode");
        } else {
            if (transactionStatus != open) {
                throw new TransactionException("Cannot commit, transaction status is " + transactionStatus);
            }
            notificationListener.transactionComplete(this);
            transactionCache.commitTransaction(transactionId, cacheKeyMap, cachePartitionMap);
        }
        transactionStatus = committed;
    }

    @Override
    public void rollback() {
        if (transactionStatus != null) {
            if (transactionStatus != open) {
                throw new TransactionException("Cannot rollback, transaction status is " + transactionStatus);
            }
            notificationListener.transactionComplete(this);
            //TODO implement blanket rollback
            transactionCache.rollbackTransaction(transactionId, cacheKeyMap, cachePartitionMap);
        }
        transactionStatus = rolledback;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

}
