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
import com.shadowmvcc.coherence.domain.TransactionId;


/**
 * Defines a source of integer transaction manager ids. Each transaction manager
 * instance must be unique within the cluster at
 * any given time.
 *
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public interface ManagerCache {

    String MGRCACHENAME = "mvcc-transaction-manager";
    
    /**
     * @return a new, unique transaction manager id
     */
    int getManagerId();
    
    /**
     * Register an MVCC cache so that client cleanup is aware of it.
     * @param id id of the manager
     * @param cacheName the logical name of the MVCC cache
     */
    void registerCache(int id, String cacheName);
    
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
