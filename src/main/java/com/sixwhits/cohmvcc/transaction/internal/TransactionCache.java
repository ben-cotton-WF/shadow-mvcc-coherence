package com.sixwhits.cohmvcc.transaction.internal;

import java.util.Map;
import java.util.Set;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.tangosol.util.Filter;

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
     * @return a {@link TransactionActualScope} specifying the isolation level,
     * transaction id, and read only flag permitted
     */
    TransactionActualScope beginTransaction(TransactionId transactionId, IsolationLevel isolationLevel);

    /**
     * Commit an open transaction.
     * @param transactionId the transaction Id
     * @param cacheKeyMap map of cache names, and keys in the caches that have been modified
     * @param cacheFilterMap map of cache names, and filters on the caches that match modified entries
     */
    void commitTransaction(TransactionId transactionId, 
            Map<CacheName, Set<Object>> cacheKeyMap, Map<CacheName, Set<Filter>> cacheFilterMap);

    /**
     * Rollback an open transaction.
     * @param transactionId the transaction Id
     * @param cacheKeyMap map of cache names, and keys in the caches that have been modified
     * @param cacheFilterMap map of cache names, and filters on the caches that match modified entries
     */
    void rollbackTransaction(TransactionId transactionId, 
            Map<CacheName, Set<Object>> cacheKeyMap, Map<CacheName, Set<Filter>> cacheFilterMap);

}
