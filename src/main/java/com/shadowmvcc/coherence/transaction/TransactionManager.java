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

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.cache.internal.MVCCNamedCache;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.TransactionId;

/**
 * {@code TransactionManager} implementations are responsible for creating {@link Transaction}
 * objects. Instances of {@link MVCCNamedCache} must be obtained from a {@code TransactionManager}
 * so that cache operations are performed withing the correct transaction.
 *
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public interface TransactionManager {
    
    /**
     * Default name for the invocation service.
     */
    String DEFAULT_INVOCATION_SERVICE_NAME = "InvocationService";

    /**
     * Construct and return an {@link MVCCNamedCache}.
     * @param cacheName the cache name
     * @return the cache
     */
    MVCCNamedCache getCache(String cacheName);

    /**
     * Return the current transaction, constructs a new transaction if required.
     * @return the transaction
     */
    Transaction getTransaction();

    /**
     * Return true if a transaction has been started. Transactions are implicitly started by the
     * first cache operation, and removed after commit or rollback.
     * @return true if a transaction has been started.
     */
    boolean isTransactionOpen();

    /**
     * Set the isolation level for new transactions. Does not affect any transaction in progress.
     * @param isolationLevel the new isolation level.
     */
    void setIsolationLevel(IsolationLevel isolationLevel);

    /**
     * @return the current isolation level
     */
    IsolationLevel getIsolationLevel();

    /**
     * Set the  transaction manager in autocommit mode? If true, each cache operation creates
     * a new transaction, which is implicitly committed, Does not affect any currently open transaction
     * @param autoCommit true to set autocommit mode, false to clear
     */
    void setAutoCommit(boolean autoCommit);

    /**
     * @return true if in autommit mode
     */
    boolean isAutoCommit();

    /**
     * Set transactions to read-only mode. Any update operation will fail. Does not affect
     * currently active transaction.
     * @param readOnly true to set read-only mode. False to clear
     */
    void setReadOnly(boolean readOnly);

    /**
     * @return true if the manager is in read only mode
     */
    boolean isReadOnly();

    /**
     * Establish a snapshot, removing old versions up to the specified snapshot timestamp.
     * Only the most recent version of each entry newer than the previous snapshot and before the
     * given snapshot id are retained, all other versions in the interval are deleted and all
     * read markers up to the snapshot time are deleted. No updates will be accepted henceforth
     * with a transaction id earlier than the most recent snapshot
     * @param cacheName cache to apply the snapshot to
     * @param snapshotId the transactionId to establish the new snapshot
     * @return the next earlier snapshot timestamp.
     * @throws IllegalArgumentException if there are open transactions with
     * timestamp earlier than the requested snapshot.
     */
    TransactionId createSnapshot(CacheName cacheName, TransactionId snapshotId);
    
    /**
     * Coalesce snapshots, removing intermediate snapshots so that only the most
     * recent cache entry within the interval is retained.
     * @param cacheName cache to apply the snapshot coalescence to
     * @param fromSnapshotId the earliest snapshot to retain
     * @param toSnapshotId the latest snapshot to retain
     * @throws IllegalArgumentException if either snapshot does not exist
     * or if the to snapshot is not later than the from snapshot
     */
    void coalesceSnapshots(CacheName cacheName,
            TransactionId fromSnapshotId, TransactionId toSnapshotId);
}
