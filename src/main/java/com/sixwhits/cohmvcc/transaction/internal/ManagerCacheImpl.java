package com.sixwhits.cohmvcc.transaction.internal;

import com.sixwhits.cohmvcc.transaction.ManagerCache;
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
    private static final int KEY = 0;

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
}
