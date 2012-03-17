package com.sixwhits.cohmvcc.transaction.internal;

import static com.sixwhits.cohmvcc.domain.TransactionStatus.committed;
import static com.sixwhits.cohmvcc.domain.TransactionStatus.open;
import static com.sixwhits.cohmvcc.domain.TransactionStatus.rolledback;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.TransactionStatus;
import com.sixwhits.cohmvcc.transaction.Transaction;
import com.sixwhits.cohmvcc.transaction.TransactionException;
import com.sixwhits.cohmvcc.transaction.TransactionNotificationListener;
import com.tangosol.util.Filter;

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
    private volatile boolean rollbackOnly = false;
    private volatile TransactionStatus transactionStatus = null;

    private Map<CacheName, Set<Object>> cacheKeyMap = new HashMap<CacheName, Set<Object>>();
    private Map<CacheName, Set<Filter>> cacheFilterMap = new HashMap<CacheName, Set<Filter>>();

    /**
     * Constructor.
     * @param transactionId transaction id
     * @param isolationLevel isolation level
     * @param notificationListener listener to notify on commit or rollback
     */
    public TransactionImpl(final TransactionId transactionId, final IsolationLevel isolationLevel, 
            final TransactionNotificationListener notificationListener) {
        super();
        this.transactionId = transactionId;
        this.isolationLevel = isolationLevel;
        this.notificationListener = notificationListener;
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
            cacheKeyMap.get(cacheName).add(keys);
        }
    }

    /**
     * Add a filter to the set of affected filters for a cache.
     * @param cacheName the cache name
     * @param filter the filter
     */
    private void addCacheFilter(final CacheName cacheName, final Filter filter) {
        synchronized (cacheFilterMap) {
            if (!cacheFilterMap.containsKey(cacheName)) {
                cacheFilterMap.put(cacheName, new HashSet<Filter>());
            }
            cacheKeyMap.get(cacheName).add(filter);
        }
    }

    @Override
    public void addKeyAffected(final CacheName cacheName, final Object key) {
        checkTransactionInitialised();
        addCacheKey(cacheName, key);
    }

    /**
     * Lazily initialise the transaction.
     */
    private void checkTransactionInitialised() {
        if (transactionStatus == null) {
            synchronized (this) {
                if (transactionStatus == null) {
                    transactionStatus = open;
                    // Create transaction cache entry
                }
            }
        }
    }

    @Override
    public void addKeySetAffected(final CacheName cacheName, final Collection<Object> keys) {
        checkTransactionInitialised();
        addCacheKeys(cacheName, keys);
    }

    @Override
    public int addFilterAffected(final CacheName cacheName, final Filter filter) {
        checkTransactionInitialised();
        addCacheFilter(cacheName, filter);
        return 0;
    }

    @Override
    public void filterKeysAffected(final int invocationId, final Collection<?> keys) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void filterPartitionsAffected(final int invocationId, 
            final Collection<Integer> keys) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("not yet implemented");
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
            //Do the commit
        }
        transactionStatus = committed;
    }

    @Override
    public void rollback() {
        if (transactionStatus != null) {
            if (transactionStatus != rolledback) {
                throw new TransactionException("Cannot rollback, transaction status is " + transactionStatus);
            }
            notificationListener.transactionComplete(this);
            //Do the rollback
        }
        transactionStatus = rolledback;
    }

}
