package com.shadowmvcc.coherence.extend;

import static com.shadowmvcc.coherence.domain.IsolationLevel.readProhibited;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.shadowmvcc.coherence.cache.MVCCTransactionalCache;
import com.shadowmvcc.coherence.cache.internal.AbstractMVCCTransactionalCache;
import com.shadowmvcc.coherence.cache.internal.InvocationFinalResult;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.Invocable;
import com.tangosol.net.InvocationService;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.EntryAggregator;
import com.tangosol.util.InvocableMap.EntryProcessor;
import com.tangosol.util.ValueExtractor;

/**
 * Implementation of MVCCTransactionalCache for extend clients.
 * All cache read/write operations are performed remotely
 * on a proxy server by invocation service.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> cache key type
 * @param <V> cache value type
 */
public class MVCCExtendTransactionalCache<K, V> extends AbstractMVCCTransactionalCache<K, V>
        implements MVCCTransactionalCache<K, V> {
    
    private final String invocationServiceName;

    /**
     * Constructor.
     * @param cacheName logical cache name
     * @param invocationServiceName name of the remote invocation service
     */
    public MVCCExtendTransactionalCache(final String cacheName, final String invocationServiceName) {
        super(cacheName);
        this.invocationServiceName = invocationServiceName;
    }

    @Override
    public V get(final TransactionId tid, final IsolationLevel isolationLevel, final K key) {
        
        ExtendCacheInvoker<K, V> invoker = new ExtendCacheInvoker<K, V>(
                cacheName, ExtendCacheInvoker.Operation.get, tid, isolationLevel);
        
        invoker.setKey(key);
        
        return invoke(invoker);
    }
    
    /**
     * Send the invocation to the proxy invocation service.
     * @param invocable the invocable to send
     * @param <R> result type of invocation
     * @return result of invocation
     */
    @SuppressWarnings("unchecked")
    private <R> R invoke(final Invocable invocable) {
        
        InvocationService service = (InvocationService) CacheFactory.getService(invocationServiceName);
        
        return (R) service.query(invocable, null);
    }

    @Override
    public V put(final TransactionId tid, final IsolationLevel isolationLevel,
            final boolean autoCommit, final K key, final V value) {
        
        ExtendCacheInvoker<K, V> invoker = new ExtendCacheInvoker<K, V>(
                cacheName, ExtendCacheInvoker.Operation.get, tid, isolationLevel);
        
        invoker.setAutoCommit(autoCommit);
        invoker.setKey(key);
        invoker.setValue(value);

        return invoke(invoker);
    }

    @Override
    public void insert(final TransactionId tid, final boolean autoCommit, final K key, final V value) {

        ExtendCacheInvoker<K, V> invoker = new ExtendCacheInvoker<K, V>(
                cacheName, ExtendCacheInvoker.Operation.insert, tid, readProhibited);
        
        invoker.setAutoCommit(autoCommit);
        invoker.setKey(key);
        invoker.setValue(value);

        invoke(invoker);
    }

    @Override
    public <R> InvocationFinalResult<K, R> invoke(final TransactionId tid,
            final IsolationLevel isolationLevel, final boolean autoCommit,
            final boolean readonly, final K key, final EntryProcessor agent) {

        ExtendCacheInvoker<K, V> invoker = new ExtendCacheInvoker<K, V>(
                cacheName, ExtendCacheInvoker.Operation.invoke, tid, isolationLevel);
        
        invoker.setAutoCommit(autoCommit);
        invoker.setReadOnly(readonly);
        invoker.setKey(key);
        invoker.setEntryProcessor(agent);

        return invoke(invoker);
    }

    @Override
    public int size(final TransactionId tid, final IsolationLevel isolationLevel)
            throws Throwable {

        ExtendCacheInvoker<K, V> invoker = new ExtendCacheInvoker<K, V>(
                cacheName, ExtendCacheInvoker.Operation.size, tid, isolationLevel);
        
        return invoke(invoker);
    }

    @Override
    public boolean isEmpty(final TransactionId tid, final IsolationLevel isolationLevel)
            throws Throwable {

        ExtendCacheInvoker<K, V> invoker = new ExtendCacheInvoker<K, V>(
                cacheName, ExtendCacheInvoker.Operation.isEmpty, tid, isolationLevel);
        
        return invoke(invoker);
    }

    @Override
    public boolean containsKey(final TransactionId tid,
            final IsolationLevel isolationLevel, final K key) {

        ExtendCacheInvoker<K, V> invoker = new ExtendCacheInvoker<K, V>(
                cacheName, ExtendCacheInvoker.Operation.containsKey, tid, isolationLevel);
        
        invoker.setKey(key);
        
        return invoke(invoker);
    }

    @Override
    public boolean containsValue(final TransactionId tid,
            final IsolationLevel isolationLevel, final V value) throws Throwable {

        ExtendCacheInvoker<K, V> invoker = new ExtendCacheInvoker<K, V>(
                cacheName, ExtendCacheInvoker.Operation.containsValue, tid, isolationLevel);
        
        invoker.setValue(value);
        
        return invoke(invoker);
    }

    @Override
    public V remove(final TransactionId tid, final IsolationLevel isolationLevel,
            final boolean autoCommit, final K key) {

        ExtendCacheInvoker<K, V> invoker = new ExtendCacheInvoker<K, V>(
                cacheName, ExtendCacheInvoker.Operation.remove, tid, isolationLevel);
        
        invoker.setAutoCommit(autoCommit);
        invoker.setKey(key);
        
        return invoke(invoker);
    }

    @Override
    public void putAll(final TransactionId tid, final boolean autoCommit, final Map<K, V> m) {

        ExtendCacheInvoker<K, V> invoker = new ExtendCacheInvoker<K, V>(
                cacheName, ExtendCacheInvoker.Operation.putAll, tid, readProhibited);
        
        invoker.setAutoCommit(autoCommit);
        invoker.setKeyValueMap(m);
        
        invoke(invoker);
    }

    @Override
    public void clear(final TransactionId tid, final boolean autoCommit) throws Throwable {

        ExtendCacheInvoker<K, V> invoker = new ExtendCacheInvoker<K, V>(
                cacheName, ExtendCacheInvoker.Operation.clear, tid, readProhibited);
        
        invoker.setAutoCommit(autoCommit);
        invoke(invoker);
    }

    @Override
    public Set<K> keySet(final TransactionId tid, final IsolationLevel isolationLevel)
            throws Throwable {

        ExtendCacheInvoker<K, V> invoker = new ExtendCacheInvoker<K, V>(
                cacheName, ExtendCacheInvoker.Operation.keySet, tid, isolationLevel);
        
        return invoke(invoker);
    }

    @Override
    public Collection<V> values(final TransactionId tid, final IsolationLevel isolationLevel)
            throws Throwable {

        ExtendCacheInvoker<K, V> invoker = new ExtendCacheInvoker<K, V>(
                cacheName, ExtendCacheInvoker.Operation.values, tid, isolationLevel);
        
        return invoke(invoker);
    }

    @Override
    public Set<Entry<K, V>> entrySet(final TransactionId tid,
            final IsolationLevel isolationLevel) throws Throwable {

        ExtendCacheInvoker<K, V> invoker = new ExtendCacheInvoker<K, V>(
                cacheName, ExtendCacheInvoker.Operation.entrySet, tid, isolationLevel);
        
        return invoke(invoker);
    }

    @Override
    public Map<K, V> getAll(final TransactionId tid, final IsolationLevel isolationLevel,
            final Collection<K> colKeys) {

        ExtendCacheInvoker<K, V> invoker = new ExtendCacheInvoker<K, V>(
                cacheName, ExtendCacheInvoker.Operation.getAll, tid, isolationLevel);
        
        invoker.setColKeys(colKeys);
        
        return invoke(invoker);
    }

    @Override
    public void addIndex(final ValueExtractor extractor, final boolean fOrdered,
            final Comparator<V> comparator) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<Entry<K, V>> entrySet(final TransactionId tid,
            final IsolationLevel isolationLevel, final Filter filter) throws Throwable {

        ExtendCacheInvoker<K, V> invoker = new ExtendCacheInvoker<K, V>(
                cacheName, ExtendCacheInvoker.Operation.entrySetFilter, tid, isolationLevel);
        
        invoker.setFilter(filter);
        
        return invoke(invoker);
    }

    @Override
    public Set<Entry<K, V>> entrySet(final TransactionId tid,
            final IsolationLevel isolationLevel, final Filter filter,
            final Comparator<V> comparator) {

        ExtendCacheInvoker<K, V> invoker = new ExtendCacheInvoker<K, V>(
                cacheName, ExtendCacheInvoker.Operation.entrySetComparator, tid, isolationLevel);
        
        invoker.setFilter(filter);
        invoker.setComparator(comparator);
        
        return invoke(invoker);
    }

    @Override
    public Set<K> keySet(final TransactionId tid, final IsolationLevel isolationLevel,
            final Filter filter) throws Throwable {

        ExtendCacheInvoker<K, V> invoker = new ExtendCacheInvoker<K, V>(
                cacheName, ExtendCacheInvoker.Operation.keySetFilter, tid, isolationLevel);
        
        invoker.setFilter(filter);
        
        return invoke(invoker);
    }

    @Override
    public void removeIndex(final ValueExtractor extractor) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public <R> R aggregate(final TransactionId tid, final IsolationLevel isolationLevel,
            final Collection<K> collKeys, final EntryAggregator agent) throws Throwable {

        ExtendCacheInvoker<K, V> invoker = new ExtendCacheInvoker<K, V>(
                cacheName, ExtendCacheInvoker.Operation.aggregateKeys, tid, isolationLevel);
        
        invoker.setColKeys(collKeys);
        invoker.setAggregator(agent);
        
        return invoke(invoker);
    }

    @Override
    public <R> R aggregate(final TransactionId tid, final IsolationLevel isolationLevel,
            final Filter filter, final EntryAggregator agent) throws Throwable {

        ExtendCacheInvoker<K, V> invoker = new ExtendCacheInvoker<K, V>(
                cacheName, ExtendCacheInvoker.Operation.aggregateFilter, tid, isolationLevel);
        
        invoker.setFilter(filter);
        invoker.setAggregator(agent);
        
        return invoke(invoker);
    }

    @Override
    public <R> InvocationFinalResult<K, R> invokeAll(final TransactionId tid,
            final IsolationLevel isolationLevel, final boolean autoCommit,
            final boolean readonly, final Collection<K> collKeys, final EntryProcessor agent) {

        ExtendCacheInvoker<K, V> invoker = new ExtendCacheInvoker<K, V>(
                cacheName, ExtendCacheInvoker.Operation.invokeAllKeys, tid, isolationLevel);
        
        invoker.setAutoCommit(autoCommit);
        invoker.setReadOnly(readonly);
        invoker.setColKeys(collKeys);
        invoker.setEntryProcessor(agent);
        
        return invoke(invoker);
    }

    @Override
    public <R> InvocationFinalResult<K, R> invokeAll(final TransactionId tid,
            final IsolationLevel isolationLevel, final boolean autoCommit,
            final boolean readonly, final Filter filter, final EntryProcessor agent)
            throws Throwable {

        ExtendCacheInvoker<K, V> invoker = new ExtendCacheInvoker<K, V>(
                cacheName, ExtendCacheInvoker.Operation.invokeAllKeys, tid, isolationLevel);
        
        invoker.setAutoCommit(autoCommit);
        invoker.setReadOnly(readonly);
        invoker.setFilter(filter);
        invoker.setEntryProcessor(agent);
        
        return invoke(invoker);
    }
}
