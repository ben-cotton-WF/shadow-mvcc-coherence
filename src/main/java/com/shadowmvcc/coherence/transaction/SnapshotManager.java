package com.shadowmvcc.coherence.transaction;

import java.util.NavigableSet;

import com.shadowmvcc.coherence.cache.CacheName;

/**
 * Interface defining the operations of creating and removing snapshots for
 * a cache. Snapshots are created by removing all entries from the version cache other
 * than the most recent before the snapshot timestamp. Queries are therefore only
 * valid at timestamp times or at times more recent than the most recent snapshot.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public interface SnapshotManager {

    /**
     * Coalesce snapshots, removing all earlier snapshots so that only the most
     * recent cache entry is retained.
     * @param cacheName cache to apply the snapshot coalescence to
     * @param toSnapshotTime the latest snapshot to retain
     * @throws IllegalArgumentException if either snapshot does not exist
     * or if the to snapshot is not later than the from snapshot
     */
    void coalesceSnapshots(CacheName cacheName, long toSnapshotTime);

    /**
     * Coalesce snapshots, removing intermediate snapshots so that only the most
     * recent cache entry within the interval is retained.
     * @param cacheName cache to apply the snapshot coalescence to
     * @param fromSnapshotTime the earliest snapshot to retain
     * @param toSnapshotTime the latest snapshot to retain
     * @throws IllegalArgumentException if either snapshot does not exist
     * or if the to snapshot is not later than the from snapshot
     */
    void coalesceSnapshots(CacheName cacheName,
            long fromSnapshotTime, long toSnapshotTime);

    /**
     * Establish a snapshot, removing old versions up to the specified snapshot timestamp.
     * Only the most recent version of each entry newer than the previous snapshot and before the
     * given snapshot id are retained, all other versions in the interval are deleted and all
     * read markers up to the snapshot time are deleted. No updates will be accepted henceforth
     * with a transaction id earlier than the most recent snapshot
     * @param cacheName cache to apply the snapshot to
     * @param snapshotTime the time in millis to establish the new snapshot
     * @return the next earlier snapshot timestamp.
     * @throws IllegalArgumentException if there are open transactions with
     * timestamp earlier than the requested snapshot.
     */
    long createSnapshot(CacheName cacheName, long snapshotTime);
    
    /**
     * Get the collection of snapshot timestamps defined for a cache.
     * @param cacheName name of the cache
     * @return an order set of timestamps
     */
    NavigableSet<Long> getValidSnapshots(CacheName cacheName);
    
   

}
