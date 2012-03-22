package com.sixwhits.cohmvcc.transaction.internal;

import com.sixwhits.cohmvcc.transaction.ManagerIdSource;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;

/**
 * Implementation of {@link ManagerIdSource} that obtains ids from
 * a cache.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class ManagerIdSourceImpl implements ManagerIdSource {

    private static final String CACHENAME = "transaction-manager-id";
    private static final int KEY = 0;

    @Override
    public int getManagerId() {

        NamedCache managerIdCache = CacheFactory.getCache(CACHENAME);

        return (Integer) managerIdCache.invoke(KEY, CounterProcessor.INSTANCE);

    }
}
