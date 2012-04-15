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

package com.shadowmvcc.coherence.transaction;

import java.util.Collection;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.TransactionId;
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
