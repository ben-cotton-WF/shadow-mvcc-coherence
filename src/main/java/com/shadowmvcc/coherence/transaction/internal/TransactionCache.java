package com.shadowmvcc.coherence.transaction.internal;

import java.util.Map;
import java.util.Set;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.tangosol.net.partition.PartitionSet;

/**
 * Cache persistence of transaction state.
 *
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public interface TransactionCache {

    String CACHENAME = "mvcc-transaction";
    /**
     * Store an open transaction event. Must be called before any other cache updates for this transactionId
     *
     * @param transactionId the transaction Id being started
     * @param isolationLevel requested isolation level
     * transaction id, and read only flag permitted
     */
    void beginTransaction(TransactionId transactionId, IsolationLevel isolationLevel);

    /**
     * Commit an open transaction.
     * @param transactionId the transaction Id
     * @param cacheKeyMap map of cache names, and keys in the caches that have been modified
     * @param cachePartitionMap map of cache names, and partitions on the caches that contain many modified entries
     */
    void commitTransaction(TransactionId transactionId, 
            Map<CacheName, Set<Object>> cacheKeyMap, Map<CacheName, PartitionSet> cachePartitionMap);

    /**
     * Rollback an open transaction.
     * @param transactionId the transaction Id
     * @param cacheKeyMap map of cache names, and keys in the caches that have been modified
     * @param cachePartitionMap map of cache names, and partitions on the caches that contain many modified entries
     */
    void rollbackTransaction(TransactionId transactionId, 
            Map<CacheName, Set<Object>> cacheKeyMap, Map<CacheName, PartitionSet> cachePartitionMap);

}
