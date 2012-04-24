package com.shadowmvcc.coherence.transaction.internal;

import java.util.NavigableSet;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.transaction.ManagerCache;
import com.shadowmvcc.coherence.transaction.SnapshotManager;

/**
 * Implementation of {@link SnapshotManager}.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class SnapshotManagerImpl implements SnapshotManager {

    private final ManagerCache managerCache;

    /**
     * Constructor.
     */
    public SnapshotManagerImpl() {
        super();
        this.managerCache = getManagerCache();
    }

    /**
     * Protected method for obtaining the {@link ManagerCache} instance.
     * @return the ManagerCache
     */
    protected ManagerCache getManagerCache() {
        return new ManagerCacheImpl();
    }

    @Override
    public long createSnapshot(final CacheName cacheName,
            final long snapshotTime) {
        // TODO verify no open transactions at or earlier than snapshotId
        TransactionId snapshotId = new TransactionId(snapshotTime, Integer.MAX_VALUE, Integer.MAX_VALUE);
        return managerCache.createSnapshot(cacheName, snapshotId).getTimeStampMillis();
    }

    @Override
    public void coalesceSnapshots(final CacheName cacheName,
            final long fromSnapshotTime, final long toSnapshotTime) {
        TransactionId fromSnapshotId = new TransactionId(fromSnapshotTime, Integer.MAX_VALUE, Integer.MAX_VALUE);
        TransactionId toSnapshotId = new TransactionId(toSnapshotTime, Integer.MAX_VALUE, Integer.MAX_VALUE);
        managerCache.coalesceSnapshots(cacheName, fromSnapshotId, toSnapshotId);
    }

    @Override
    public void coalesceSnapshots(final CacheName cacheName,
            final long toSnapshotTime) {
        TransactionId toSnapshotId = new TransactionId(toSnapshotTime, Integer.MAX_VALUE, Integer.MAX_VALUE);
        managerCache.coalesceSnapshots(cacheName, null, toSnapshotId);
    }


    @Override
    public NavigableSet<Long> getValidSnapshots(final CacheName cacheName) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("not yet implemented");
    }

}
