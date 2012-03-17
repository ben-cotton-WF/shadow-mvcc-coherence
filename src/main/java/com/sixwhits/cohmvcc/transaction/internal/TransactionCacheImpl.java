package com.sixwhits.cohmvcc.transaction.internal;

import static com.sixwhits.cohmvcc.domain.TransactionCacheValue.TransactionProcStatus.committing;
import static com.sixwhits.cohmvcc.domain.TransactionCacheValue.TransactionProcStatus.open;
import static com.sixwhits.cohmvcc.domain.TransactionCacheValue.TransactionProcStatus.rollingback;

import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.Constants;
import com.sixwhits.cohmvcc.domain.TransactionCacheValue;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.transaction.TransactionCache;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.EntryProcessor;
import com.tangosol.util.filter.AndFilter;
import com.tangosol.util.filter.AnyFilter;
import com.tangosol.util.filter.EqualsFilter;

/**
 * Cache service facade implementation for transaction operations in the grid.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class TransactionCacheImpl implements TransactionCache {

    private static final String CACHENAME = "mvcc-transaction";

    @Override
    public void beginTransaction(final TransactionId transactionId) {
        NamedCache transactionCache = CacheFactory.getCache(CACHENAME);
        transactionCache.put(transactionId, new TransactionCacheValue(open, new Date().getTime()));
    }

    @Override
    public void commitTransaction(final TransactionId transactionId, 
            final Map<CacheName, Set<Object>> cacheKeyMap, 
            final Map<CacheName, Set<Filter>> cacheFilterMap) {
        NamedCache transactionCache = CacheFactory.getCache(CACHENAME);
        transactionCache.put(transactionId, new TransactionCacheValue(committing, new Date().getTime()));

        actionTransaction(transactionId, cacheKeyMap, cacheFilterMap, EntryCommitProcessor.INSTANCE);

        transactionCache.remove(transactionId);
    }

    @Override
    public void rollbackTransaction(final TransactionId transactionId, 
            final Map<CacheName, Set<Object>> cacheKeyMap, 
            final Map<CacheName, Set<Filter>> cacheFilterMap) {
        NamedCache transactionCache = CacheFactory.getCache(CACHENAME);
        transactionCache.put(transactionId, new TransactionCacheValue(rollingback, new Date().getTime()));

        actionTransaction(transactionId, cacheKeyMap, cacheFilterMap, EntryRollbackProcessor.INSTANCE);

        transactionCache.remove(transactionId);
    }

    /**
     * Perform the update or remove operations to effect the
     * commit or rollback on all the cache entries belonging to
     * this transaction.
     * @param transactionId the transaction id
     * @param cacheKeyMap map of affected caches to sets of keys
     * @param cacheFilterMap map of affected caches to sets of filters
     * @param agent the remove or update processor to invoke
     */
    private void actionTransaction(final TransactionId transactionId, 
            final Map<CacheName, Set<Object>> cacheKeyMap, 
            final Map<CacheName, Set<Filter>> cacheFilterMap, 
            final EntryProcessor agent) {

        for (Map.Entry<CacheName, Set<Object>> cacheKeyEntry : cacheKeyMap.entrySet()) {
            NamedCache vcache = CacheFactory.getCache(cacheKeyEntry.getKey().getVersionCacheName());
            Set<VersionedKey<Object>> vkeys = new HashSet<VersionedKey<Object>>();
            for (Object key : cacheKeyEntry.getValue()) {
                vkeys.add(new VersionedKey<Object>(key, transactionId));
            }
            vcache.invokeAll(vkeys, agent);
        }

        Filter versionFilter = new EqualsFilter(Constants.KEYEXTRACTOR, transactionId);
        for (Map.Entry<CacheName, Set<Filter>> cacheFilterEntry : cacheFilterMap.entrySet()) {
            NamedCache vcache = CacheFactory.getCache(cacheFilterEntry.getKey().getVersionCacheName());
            Filter anyFilter = new AnyFilter(cacheFilterEntry.getValue().toArray(
                    new Filter[cacheFilterEntry.getValue().size()]));
            Filter andFilter = new AndFilter(versionFilter, anyFilter);
            vcache.invokeAll(andFilter, agent);
        }
    }
}
