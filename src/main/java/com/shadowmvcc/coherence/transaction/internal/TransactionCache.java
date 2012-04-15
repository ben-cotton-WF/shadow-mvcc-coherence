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
