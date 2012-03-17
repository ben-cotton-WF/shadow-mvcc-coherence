package com.sixwhits.cohmvcc.transaction;

import java.util.Collection;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.tangosol.util.Filter;

/**
 * Transaction Context containing all state pertaining to an open transaction.
 *
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public interface Transaction {

    /**
     * @return the id of the current transaction.
     */
    TransactionId getTransactionId();
    /**
     * @return the isolation level of the current transaction
     */
    IsolationLevel getIsolationLevel();

    /**
     * @return true if this is an autocommit context.
     */
    boolean isAutoCommit();

    /**
     * Add a cache name and key that may have been changed in this transaction.
     * @param cacheName the name of the changed cache
     * @param key the key affected by the change
     */
    void addKeyAffected(CacheName cacheName, Object key);
    
    /**
     * Add a cache name and collection of keys that may have been changed in this transaction.
     * @param cacheName the name of the changed cache
     * @param keys the key affected by the change
     */
    void addKeySetAffected(CacheName cacheName, Collection<Object> keys);

    /**
     * Add a cache name and filter that have used to effect changes. Returns an invocation id that may be used
     * in a subsequent call to {@code filterKeysAffected} or {@code filterPartitionsAffected} to replace the
     * recorded filter.
     * @param cacheName the name of the changed cache
     * @param filter the filter used to find entries to change
     * @return the invocation id
     */
    int addFilterAffected(CacheName cacheName, Filter filter);

    /**
     * Provide the collection of keys affected by a filter operation.
     * @param invocationId identifies the previously notified filter
     * @param keys the keys affected
     */
    void filterKeysAffected(int invocationId, Collection<?> keys);

    /**
     * Provide the collection of partitions affected by a filter operation.
     * @param invocationId identifies the previously notified filter
     * @param keys the keys affected
     */
    void filterPartitionsAffected(int invocationId, Collection<Integer> keys);

    /**
     * Set the transaction in a rollback-only mode. An attempt to commit
     * will throw an exception.
     */
    void setRollbackOnly();

    /**
     * Commit the transaction.
     */
    void commit();

    /**
     * Roll back the transaction.
     */
    void rollback();

}
