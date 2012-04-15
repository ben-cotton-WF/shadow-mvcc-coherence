package com.shadowmvcc.coherence.transaction.internal;

import java.util.SortedSet;
import java.util.TreeSet;

import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.invocable.SortedSetAppender;
import com.shadowmvcc.coherence.transaction.ManagerCache;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;

/**
 * Implementation of {@link ManagerCache} that obtains ids from
 * a cache.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class ManagerCacheImpl implements ManagerCache {

    private static final String IDCACHENAME = "mvcc-transaction-manager-id";
    private static final String SNAPSHOTCACHENAME = "mvcc-snapshot";
    private static final int KEY = 0;
    private static final SortedSet<TransactionId> INITIAL_SNAPSHOTS;
    
    static {
        INITIAL_SNAPSHOTS = new TreeSet<TransactionId>();
        INITIAL_SNAPSHOTS.add(BIG_BANG);
    }

    @Override
    public int getManagerId() {

        NamedCache managerIdCache = CacheFactory.getCache(IDCACHENAME);
        
        return (Integer) managerIdCache.invoke(KEY, CounterProcessor.INSTANCE);

    }

    @Override
    public void registerCache(final int managerId, final String cacheName) {
        
        NamedCache managerCache = CacheFactory.getCache(MGRCACHENAME);
        managerCache.invoke(managerId, new CacheRegistration(cacheName));
        
    }

    @Override
    public TransactionId createSnapshot(final String cacheName, final TransactionId snapshotId) {
        NamedCache snapshotCache = CacheFactory.getCache(SNAPSHOTCACHENAME);
        if (!(Boolean) snapshotCache.invoke(snapshotCache,
                new SortedSetAppender<TransactionId>(INITIAL_SNAPSHOTS, snapshotId))) {
            //TODO more informative error
            throw new IllegalArgumentException("illegal snapshot timestamp " + snapshotId);
        }
        throw new UnsupportedOperationException("not yet implemented");
    }
    
    @Override
    public void coalesceSnapshots(final String cacheName,
            final TransactionId precedingSnapshotId, final TransactionId snapshotId) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("not yet implemented");
    }
}
