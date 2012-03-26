package com.sixwhits.cohmvcc.transaction;

import java.util.Collection;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.tangosol.net.partition.PartitionSet;

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
     * Add a set of partitions for a cache that may have been changed in this transaction.
     * Used when the number of keys affected per partition is sufficiently large that
     * it is more efficient to update by filtering on partition.
     * @param cacheName the cache name
     * @param partitionSet the set of affected partitions
     */
    void addPartitionSetAffected(CacheName cacheName, PartitionSet partitionSet);

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
