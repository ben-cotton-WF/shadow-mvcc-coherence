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

import java.util.SortedSet;
import java.util.TreeSet;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.index.MVCCSnapshotPurgeFilter;
import com.shadowmvcc.coherence.invocable.SortedSetAppender;
import com.shadowmvcc.coherence.transaction.ManagerCache;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.Filter;
import com.tangosol.util.filter.AlwaysFilter;
import com.tangosol.util.processor.ConditionalRemove;

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
        INITIAL_SNAPSHOTS.add(TransactionId.BIG_BANG);
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
    public TransactionId createSnapshot(final CacheName cacheName, final TransactionId snapshotId) {
        NamedCache snapshotCache = CacheFactory.getCache(SNAPSHOTCACHENAME);
        TransactionId rangeStart = (TransactionId) snapshotCache.invoke(cacheName.getLogicalName(),
                new SortedSetAppender<TransactionId>(INITIAL_SNAPSHOTS, snapshotId));
        if (rangeStart == null) {
            //TODO more informative error
            throw new IllegalArgumentException("illegal snapshot timestamp " + snapshotId);
        }
        
        NamedCache versionCache = CacheFactory.getCache(cacheName.getVersionCacheName());
        
        Filter purgeFilter = new MVCCSnapshotPurgeFilter<Object>(rangeStart, snapshotId);
        
        versionCache.invokeAll(purgeFilter, new ConditionalRemove(AlwaysFilter.INSTANCE));
        
        return rangeStart;
        
    }
    
    @Override
    public void coalesceSnapshots(final CacheName cacheName,
            final TransactionId precedingSnapshotId, final TransactionId snapshotId) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("not yet implemented");
    }
}
