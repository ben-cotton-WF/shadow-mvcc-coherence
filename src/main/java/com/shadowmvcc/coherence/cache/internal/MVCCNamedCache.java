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

package com.shadowmvcc.coherence.cache.internal;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.cache.MVCCTransactionalCache;
import com.shadowmvcc.coherence.invocable.MultiCacheProcessor;
import com.shadowmvcc.coherence.transaction.Transaction;
import com.shadowmvcc.coherence.transaction.TransactionException;
import com.shadowmvcc.coherence.transaction.TransactionManager;
import com.tangosol.coherence.component.util.daemon.queueProcessor.service.grid.partitionedService.PartitionedCache;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.CacheService;
import com.tangosol.net.NamedCache;
import com.tangosol.net.partition.PartitionSet;
import com.tangosol.util.Filter;
import com.tangosol.util.MapListener;
import com.tangosol.util.ValueExtractor;

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
        if (context.isExpired()) {
            throw new TransactionException("Transaction " + context.getTransactionId() + " has expired");
        }
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
        if (context.isExpired()) {
            throw new TransactionException("Transaction " + context.getTransactionId() + " has expired");
        }
        try {
            return mvccCache.size(context.getTransactionId(), context.getIsolationLevel());
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isEmpty() {
        Transaction context = transactionManager.getTransaction();
        if (context.isExpired()) {
            throw new TransactionException("Transaction " + context.getTransactionId() + " has expired");
        }
        try {
            return mvccCache.isEmpty(context.getTransactionId(), context.getIsolationLevel());
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean containsKey(final Object key) {
        Transaction context = transactionManager.getTransaction();
        if (context.isExpired()) {
            throw new TransactionException("Transaction " + context.getTransactionId() + " has expired");
        }
        return mvccCache.containsKey(context.getTransactionId(), context.getIsolationLevel(), key);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean containsValue(final Object value) {
        Transaction context = transactionManager.getTransaction();
        if (context.isExpired()) {
            throw new TransactionException("Transaction " + context.getTransactionId() + " has expired");
        }
        try {
            return mvccCache.containsValue(context.getTransactionId(), context.getIsolationLevel(), value);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object put(final Object key, final Object value) {
        Transaction context = transactionManager.getTransaction();
        if (context.isReadOnly()) {
            throw new TransactionException("read-only transaction");
        }
        if (context.isExpired()) {
            throw new TransactionException("Transaction " + context.getTransactionId() + " has expired");
        }
        context.addKeyAffected(mvccCache.getMVCCCacheName(), key);
        return mvccCache.put(context.getTransactionId(), context.getIsolationLevel(),
                context.isAutoCommit(), key, value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object remove(final Object key) {
        Transaction context = transactionManager.getTransaction();
        if (context.isReadOnly()) {
            throw new TransactionException("read-only transaction");
        }
        if (context.isExpired()) {
            throw new TransactionException("Transaction " + context.getTransactionId() + " has expired");
        }
        context.addKeyAffected(mvccCache.getMVCCCacheName(), key);
        return mvccCache.remove(context.getTransactionId(), context.getIsolationLevel(), context.isAutoCommit(), key);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void putAll(final Map m) {
        Transaction context = transactionManager.getTransaction();
        if (context.isReadOnly()) {
            throw new TransactionException("read-only transaction");
        }
        if (context.isExpired()) {
            throw new TransactionException("Transaction " + context.getTransactionId() + " has expired");
        }
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
        if (context.isReadOnly()) {
            throw new TransactionException("read-only transaction");
        }
        if (context.isExpired()) {
            throw new TransactionException("Transaction " + context.getTransactionId() + " has expired");
        }
        try {
            mvccCache.clear(context.getTransactionId(), context.isAutoCommit());
            context.addPartitionSetAffected(mvccCache.getMVCCCacheName(), getFullPartitionSet());
        } catch (Throwable t) {
            context.setRollbackOnly();
            throw new RuntimeException(t);
        }
    }
    
    /**
     * Get a filled partitionset for this cache.
     * @return the partition set
     */
    private PartitionSet getFullPartitionSet() {
        PartitionedCache cacheService = 
                (PartitionedCache) CacheFactory.getCache(
                        mvccCache.getMVCCCacheName().getVersionCacheName()).getCacheService();
        PartitionSet partitionSet = new PartitionSet(cacheService.getPartitionCount());
        partitionSet.fill();
        return partitionSet;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Set keySet() {
        Transaction context = transactionManager.getTransaction();
        if (context.isExpired()) {
            throw new TransactionException("Transaction " + context.getTransactionId() + " has expired");
        }
        try {
            return mvccCache.keySet(context.getTransactionId(), context.getIsolationLevel());
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Collection values() {
        Transaction context = transactionManager.getTransaction();
        if (context.isExpired()) {
            throw new TransactionException("Transaction " + context.getTransactionId() + " has expired");
        }
        try {
            return mvccCache.values(context.getTransactionId(), context.getIsolationLevel());
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Set entrySet() {
        Transaction context = transactionManager.getTransaction();
        if (context.isExpired()) {
            throw new TransactionException("Transaction " + context.getTransactionId() + " has expired");
        }
        try {
            return mvccCache.entrySet(context.getTransactionId(), context.getIsolationLevel());
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public Map getAll(final Collection colKeys) {
        Transaction context = transactionManager.getTransaction();
        if (context.isExpired()) {
            throw new TransactionException("Transaction " + context.getTransactionId() + " has expired");
        }
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
        if (context.isExpired()) {
            throw new TransactionException("Transaction " + context.getTransactionId() + " has expired");
        }
        try {
            return mvccCache.entrySet(context.getTransactionId(), context.getIsolationLevel(), filter);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public Set entrySet(final Filter filter, final Comparator comparator) {
        Transaction context = transactionManager.getTransaction();
        if (context.isExpired()) {
            throw new TransactionException("Transaction " + context.getTransactionId() + " has expired");
        }
        return mvccCache.entrySet(context.getTransactionId(), context.getIsolationLevel(), filter, comparator);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Set keySet(final Filter filter) {
        Transaction context = transactionManager.getTransaction();
        if (context.isExpired()) {
            throw new TransactionException("Transaction " + context.getTransactionId() + " has expired");
        }
        try {
            return mvccCache.keySet(context.getTransactionId(), context.getIsolationLevel(), filter);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void removeIndex(final ValueExtractor extractor) {
        mvccCache.removeIndex(extractor);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Object aggregate(final Collection collKeys, final EntryAggregator agent) {
        Transaction context = transactionManager.getTransaction();
        if (context.isExpired()) {
            throw new TransactionException("Transaction " + context.getTransactionId() + " has expired");
        }
        try {
            return mvccCache.aggregate(context.getTransactionId(), context.getIsolationLevel(), collKeys, agent);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object aggregate(final Filter filter, final EntryAggregator agent) {
        Transaction context = transactionManager.getTransaction();
        if (context.isExpired()) {
            throw new TransactionException("Transaction " + context.getTransactionId() + " has expired");
        }
        try {
            return mvccCache.aggregate(context.getTransactionId(), context.getIsolationLevel(), filter, agent);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object invoke(final Object key, final EntryProcessor agent) {
        
        Transaction context = transactionManager.getTransaction();
        if (context.isExpired()) {
            throw new TransactionException("Transaction " + context.getTransactionId() + " has expired");
        }
        
        Collection<CacheName> potentiallyAffectedCaches = null;
        if (agent instanceof MultiCacheProcessor) {
            potentiallyAffectedCaches = ((MultiCacheProcessor) agent).getReferencedMVCCCacheNames();
            transactionManager.addReferencedCaches(potentiallyAffectedCaches);
        }
        
        context.addKeyAffected(mvccCache.getMVCCCacheName(), key);
        
        @SuppressWarnings("rawtypes")
        InvocationFinalResult result = mvccCache.invoke(context.getTransactionId(),
                context.getIsolationLevel(), context.isAutoCommit(), context.isReadOnly(), key, agent);
        
        Map<CacheName, Set<Object>> changedKeys = result.getChangedKeys();
        for (Map.Entry<CacheName, Set<Object>> changedEntry : changedKeys.entrySet()) {
            context.addKeySetAffected(changedEntry.getKey(), changedEntry.getValue());
        }
        
        return result.getResultMap().get(key);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public Map invokeAll(final Collection collKeys, final EntryProcessor agent) {
        
        Transaction context = transactionManager.getTransaction();
        if (context.isExpired()) {
            throw new TransactionException("Transaction " + context.getTransactionId() + " has expired");
        }
        
        Collection<CacheName> potentiallyAffectedCaches = null;
        if (agent instanceof MultiCacheProcessor) {
            potentiallyAffectedCaches = ((MultiCacheProcessor) agent).getReferencedMVCCCacheNames();
            transactionManager.addReferencedCaches(potentiallyAffectedCaches);
        }
        
        context.addKeySetAffected(mvccCache.getMVCCCacheName(), collKeys);
        
        try {
            
            InvocationFinalResult result = mvccCache.invokeAll(context.getTransactionId(),
                    context.getIsolationLevel(), context.isAutoCommit(), context.isReadOnly(), collKeys, agent);
            
            Map<CacheName, Set<Object>> changedKeys = result.getChangedKeys();
            for (Map.Entry<CacheName, Set<Object>> changedEntry : changedKeys.entrySet()) {
                context.addKeySetAffected(changedEntry.getKey(), changedEntry.getValue());
            }
            
            return result.getResultMap();

        } catch (RuntimeException t) {
            
            context.setRollbackOnly();
            context.addPartitionSetAffected(mvccCache.getMVCCCacheName(), mvccCache.getPartitionSet());
            if (agent instanceof MultiCacheProcessor) {
                transactionManager.addReferencedCaches(potentiallyAffectedCaches);
                for (CacheName pac : potentiallyAffectedCaches) {
                    context.addPartitionSetAffected(pac, mvccCache.getPartitionSet());
                }
            }
            throw t;
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public Map invokeAll(final Filter filter, final EntryProcessor agent) {

        Transaction context = transactionManager.getTransaction();
        if (context.isExpired()) {
            throw new TransactionException("Transaction " + context.getTransactionId() + " has expired");
        }

        Collection<CacheName> potentiallyAffectedCaches = null;
        if (agent instanceof MultiCacheProcessor) {
            potentiallyAffectedCaches = ((MultiCacheProcessor) agent).getReferencedMVCCCacheNames();
            transactionManager.addReferencedCaches(potentiallyAffectedCaches);
        }
        try {
            
            InvocationFinalResult fr = mvccCache.invokeAll(context.getTransactionId(),
                    context.getIsolationLevel(), context.isAutoCommit(), context.isReadOnly(), filter, agent);
            Set<Map.Entry<CacheName, Set<Object>>> es = fr.getChangedKeys().entrySet();
            for (Map.Entry<CacheName, Set<Object>> ckEntry : es) {
                context.addKeySetAffected(ckEntry.getKey(), ckEntry.getValue());
            }
            
            return fr.getResultMap();
            
        } catch (Throwable t) {
            
            context.setRollbackOnly();
            context.addPartitionSetAffected(mvccCache.getMVCCCacheName(), mvccCache.getPartitionSet());
            
            if (agent instanceof MultiCacheProcessor) {
                potentiallyAffectedCaches = ((MultiCacheProcessor) agent).getReferencedMVCCCacheNames();
                transactionManager.addReferencedCaches(potentiallyAffectedCaches);
                for (CacheName pac : potentiallyAffectedCaches) {
                    context.addPartitionSetAffected(pac, mvccCache.getPartitionSet());
                }
            }
            
            throw new RuntimeException(t);
        }
    }

    @Override
    public void destroy() {
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
