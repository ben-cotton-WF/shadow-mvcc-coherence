package com.sixwhits.cohmvcc.cache.internal;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

import com.sixwhits.cohmvcc.cache.MVCCTransactionalCache;
import com.sixwhits.cohmvcc.transaction.Transaction;
import com.sixwhits.cohmvcc.transaction.TransactionManager;
import com.tangosol.net.CacheService;
import com.tangosol.net.NamedCache;
import com.tangosol.util.Filter;
import com.tangosol.util.MapListener;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.filter.AlwaysFilter;

/**
 * MVCC implementation of {@link NamedCache}.
 *
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class MVCCNamedCache implements NamedCache {

    private final TransactionManager transactionManager;
    @SuppressWarnings("rawtypes")
    private final MVCCTransactionalCache mvccCache;

    /**
     * @param transactionManager transaction manager used to provide transaction context
     * @param mvccTxCache MVCC cache service layer
     */
    @SuppressWarnings("rawtypes")
    public MVCCNamedCache(final TransactionManager transactionManager, final MVCCTransactionalCache mvccTxCache) {
        super();
        this.transactionManager = transactionManager;
        this.mvccCache = mvccTxCache;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object get(final Object key) {
        Transaction context = transactionManager.getTransaction();
        return mvccCache.get(context.getTransactionId(), context.getIsolationLevel(), key);
    }

    @Override
    public void addMapListener(final MapListener listener) {
        Transaction context = transactionManager.getTransaction();
        mvccCache.addMapListener(listener, context.getTransactionId(), context.getIsolationLevel());
    }

    @Override
    public void addMapListener(final MapListener listener, final Object oKey, final boolean fLite) {
        Transaction context = transactionManager.getTransaction();
        mvccCache.addMapListener(listener, context.getTransactionId(), context.getIsolationLevel(), oKey, fLite);
    }

    @Override
    public void addMapListener(final MapListener listener, final Filter filter, 
            final boolean fLite) {
        Transaction context = transactionManager.getTransaction();
        mvccCache.addMapListener(listener, context.getTransactionId(), context.getIsolationLevel(), filter, fLite);
    }

    @Override
    public void removeMapListener(final MapListener listener) {
        mvccCache.removeMapListener(listener);
    }

    @Override
    public void removeMapListener(final MapListener listener, final Object oKey) {
        mvccCache.removeMapListener(listener, oKey);
    }

    @Override
    public void removeMapListener(final MapListener listener, final Filter filter) {
        mvccCache.removeMapListener(listener, filter);
    }

    @Override
    public int size() {
        Transaction context = transactionManager.getTransaction();
        return mvccCache.size(context.getTransactionId(), context.getIsolationLevel());
    }

    @Override
    public boolean isEmpty() {
        Transaction context = transactionManager.getTransaction();
        return mvccCache.isEmpty(context.getTransactionId(), context.getIsolationLevel());
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean containsKey(final Object key) {
        Transaction context = transactionManager.getTransaction();
        return mvccCache.containsKey(context.getTransactionId(), context.getIsolationLevel(), key);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean containsValue(final Object value) {
        Transaction context = transactionManager.getTransaction();
        return mvccCache.containsValue(context.getTransactionId(), context.getIsolationLevel(), value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object put(final Object key, final Object value) {
        Transaction context = transactionManager.getTransaction();
        context.addKeyAffected(mvccCache.getMVCCCacheName(), key);
        return mvccCache.put(context.getTransactionId(), context.getIsolationLevel(),
                context.isAutoCommit(), key, value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object remove(final Object key) {
        Transaction context = transactionManager.getTransaction();
        context.addKeyAffected(mvccCache.getMVCCCacheName(), key);
        return mvccCache.remove(context.getTransactionId(), context.getIsolationLevel(), context.isAutoCommit(), key);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void putAll(final Map m) {
        Transaction context = transactionManager.getTransaction();
        context.addKeySetAffected(mvccCache.getMVCCCacheName(), m.keySet());
        try {
            mvccCache.putAll(context.getTransactionId(), context.isAutoCommit(), m);
        } catch (RuntimeException t) {
            context.setRollbackOnly();
            throw t;
        }
    }

    @Override
    public void clear() {
        Transaction context = transactionManager.getTransaction();
        context.addFilterAffected(mvccCache.getMVCCCacheName(), AlwaysFilter.INSTANCE);
        try {
            mvccCache.clear(context.getTransactionId(), context.isAutoCommit());
        } catch (RuntimeException t) {
            context.setRollbackOnly();
            throw t;
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Set keySet() {
        Transaction context = transactionManager.getTransaction();
        return mvccCache.keySet(context.getTransactionId(), context.getIsolationLevel());
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Collection values() {
        Transaction context = transactionManager.getTransaction();
        return mvccCache.values(context.getTransactionId(), context.getIsolationLevel());
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Set entrySet() {
        Transaction context = transactionManager.getTransaction();
        return mvccCache.entrySet(context.getTransactionId(), context.getIsolationLevel());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public Map getAll(final Collection colKeys) {
        Transaction context = transactionManager.getTransaction();
        return mvccCache.getAll(context.getTransactionId(), context.getIsolationLevel(), colKeys);
    }

    @Override
    public boolean lock(final Object oKey) {
        throw new UnsupportedOperationException("Locking not enabled in MVCC caches");
    }

    @Override
    public boolean lock(final Object oKey, final long cWait) {
        throw new UnsupportedOperationException("Locking not enabled in MVCC caches");
    }

    @Override
    public boolean unlock(final Object oKey) {
        throw new UnsupportedOperationException("Locking not enabled in MVCC caches");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void addIndex(final ValueExtractor extractor, final boolean fOrdered, 
            final Comparator comparator) {
        mvccCache.addIndex(extractor, fOrdered, comparator);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Set entrySet(final Filter filter) {
        Transaction context = transactionManager.getTransaction();
        return mvccCache.entrySet(context.getTransactionId(), context.getIsolationLevel(), filter);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Set entrySet(final Filter filter, final Comparator comparator) {
        throw new UnsupportedOperationException("Locking not enabled in MVCC caches");
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Set keySet(final Filter filter) {
        Transaction context = transactionManager.getTransaction();
        return mvccCache.keySet(context.getTransactionId(), context.getIsolationLevel(), filter);
    }

    @Override
    public void removeIndex(final ValueExtractor extractor) {
        mvccCache.removeIndex(extractor);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Object aggregate(final Collection collKeys, final EntryAggregator agent) {
        Transaction context = transactionManager.getTransaction();
        return mvccCache.aggregate(context.getTransactionId(), context.getIsolationLevel(), collKeys, agent);
    }

    @Override
    public Object aggregate(final Filter filter, final EntryAggregator agent) {
        Transaction context = transactionManager.getTransaction();
        return mvccCache.aggregate(context.getTransactionId(), context.getIsolationLevel(), filter, agent);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object invoke(final Object key, final EntryProcessor agent) {
        Transaction context = transactionManager.getTransaction();
        context.addKeyAffected(mvccCache.getMVCCCacheName(), key);
        return mvccCache.invoke(context.getTransactionId(),
                context.getIsolationLevel(), context.isAutoCommit(), key, agent);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public Map invokeAll(final Collection collKeys, final EntryProcessor agent) {
        Transaction context = transactionManager.getTransaction();
        context.addKeySetAffected(mvccCache.getMVCCCacheName(), collKeys);
        try {
            return mvccCache.invokeAll(context.getTransactionId(),
                    context.getIsolationLevel(), context.isAutoCommit(), collKeys, agent);
        } catch (RuntimeException t) {
            context.setRollbackOnly();
            throw t;
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map invokeAll(final Filter filter, final EntryProcessor agent) {
        Transaction context = transactionManager.getTransaction();
        context.addFilterAffected(mvccCache.getMVCCCacheName(), filter);
        try {
            Map result = mvccCache.invokeAll(context.getTransactionId(),
                    context.getIsolationLevel(), context.isAutoCommit(), filter, agent);
            return result;
        } catch (RuntimeException t) {
            context.setRollbackOnly();
            throw t;
        }
    }

    @Override
    public void destroy() {
        // TODO - force a rollback first?
        mvccCache.destroy();
    }

    @Override
    public String getCacheName() {
        return mvccCache.getCacheName();
    }

    @Override
    public CacheService getCacheService() {
        return mvccCache.getCacheService();
    }

    @Override
    public boolean isActive() {
        return mvccCache.isActive();
    }

    @Override
    public Object put(final Object oKey, final Object oValue, final long cMillis) {
        throw new UnsupportedOperationException("Expiry not enabled in MVCC caches");
    }

    @Override
    public void release() {
        mvccCache.release();
    }

}
