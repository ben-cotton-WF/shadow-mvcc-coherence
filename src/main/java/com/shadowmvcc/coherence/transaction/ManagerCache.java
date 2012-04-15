package com.shadowmvcc.coherence.transaction;

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
     * Transaction id representing the dawn of time. Implicitly exists as a
     * snapshot id.
     */
    TransactionId BIG_BANG = new TransactionId(0L, 0, 0);

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
    TransactionId createSnapshot(String cacheName, TransactionId snapshotId);
    
    /**
     * Coalesce snapshots, removing intermediate snapshots so that only the most
     * recent cache entry within the interval is retained.
     * @param cacheName cache to apply the snapshot coalescence to
     * @param fromSnapshotId the earliest snapshot to retain
     * @param toSnapshotId the latest snapshot to retain
     * @throws IllegalArgumentException if either snapshot does not exist
     * or if the to snapshot is not later than the from snapshot
     */
    void coalesceSnapshots(final String cacheName,
            final TransactionId fromSnapshotId, final TransactionId toSnapshotId);
}
