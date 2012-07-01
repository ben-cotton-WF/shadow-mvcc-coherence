package com.shadowmvcc.coherence.extend;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.cache.MVCCTransactionalCache;
import com.shadowmvcc.coherence.cache.internal.MVCCTransactionalCacheImpl;
import com.shadowmvcc.coherence.config.ConfigurationFactory;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.net.Invocable;
import com.tangosol.net.InvocationService;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.EntryAggregator;
import com.tangosol.util.InvocableMap.EntryProcessor;
import com.tangosol.util.WrapperException;

/**
 * Invoke an operation on an MVCCTransactionalCache from an extend client
 * service. Invoked from the client to run on the proxy so that cache operations
 * can be executed in the cluster.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> Key type of the cache
 * @param <V> Value type of the cache
 */
@Portable
public class ExtendCacheInvoker<K, V> implements Invocable {
    
    private static final long serialVersionUID = -1701266375519906631L;

    /**
     * Enumeration of possible operations that can be performed by 
     * the enclosing invocable.
     */
    public enum Operation { get, put, insert, invoke,
        size, isEmpty, containsKey, containsValue, remove, putAll,
        clear, keySet, values, entrySet, getAll,
        entrySetFilter, entrySetComparator, keySetFilter,
        aggregateKeys, aggregateFilter, invokeAllKeys, invokeAllFilter
        };
        
    private transient Object result = null;
    
    @PortableProperty(0) private CacheName cacheName;
    @PortableProperty(1) private Operation operation;
    @PortableProperty(2) private TransactionId transactionId;
    @PortableProperty(3) private IsolationLevel isolationLevel;
    @PortableProperty(4) private boolean readOnly;
    @PortableProperty(5) private boolean autoCommit;
    
    @PortableProperty(6) private K key = null;
    @PortableProperty(7) private V value = null;
    @PortableProperty(8) private EntryProcessor entryProcessor = null;
    @PortableProperty(9) private Map<K, V> keyValueMap = null;
    @PortableProperty(10) private Collection<K> colKeys = null;
    @PortableProperty(11) private Filter filter = null;
    @PortableProperty(12) private Comparator<V> comparator = null;
    @PortableProperty(13) private EntryAggregator aggregator = null;
    

    /**
     *  Default constructor for POF use only.
     */
    public ExtendCacheInvoker() {
        super();
    }

    /**
     * Constructor setting the fields required by all or many methods.
     * @param cacheName logical cache name
     * @param operation which method to invoke
     * @param transactionId transaction id
     * @param isolationLevel isolation level
     */
    public ExtendCacheInvoker(final CacheName cacheName,
            final Operation operation, final TransactionId transactionId,
            final IsolationLevel isolationLevel) {
        super();
        this.cacheName = cacheName;
        this.operation = operation;
        this.transactionId = transactionId;
        this.isolationLevel = isolationLevel;
    }

    /**
     * Set read only flag for methods that need it.
     * @param readOnly the read only flag
     */
    public void setReadOnly(final boolean readOnly) {
        this.readOnly = readOnly;
    }

    /**
     * Set the autocommit flag for methods that need it.
     * @param autoCommit the autocommit flag
     */
    public void setAutoCommit(final boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    /**
     * Set the key, for those methods that require it.
     * @param key the key
     */
    public void setKey(final K key) {
        this.key = key;
    }

    /**
     * Set the value, for those methods that require it.
     * @param value the value
     */
    public void setValue(final V value) {
        this.value = value;
    }

    /**
     * Set the EntryProcessor for invoke and invokeAll methods.
     * @param entryProcessor the EntryProcessor
     */
    public void setEntryProcessor(final EntryProcessor entryProcessor) {
        this.entryProcessor = entryProcessor;
    }

    /**
     * Set the key value map for putAll.
     * @param keyValueMap the map
     */
    public void setKeyValueMap(final Map<K, V> keyValueMap) {
        this.keyValueMap = keyValueMap;
    }

    /**
     * Set the collection of keys for methods that require it.
     * @param colKeys the collection of keys
     */
    public void setColKeys(final Collection<K> colKeys) {
        this.colKeys = colKeys;
    }

    /**
     * Set the filter for methods that require it.
     * @param filter the filter
     */
    public void setFilter(final Filter filter) {
        this.filter = filter;
    }

    /**
     * Set the comparator for the entrySet with comparator method.
     * @param comparator the comparator
     */
    public void setComparator(final Comparator<V> comparator) {
        this.comparator = comparator;
    }

    /**
     * Set the aggregator for aggregate methods.
     * @param aggregator the aggregator
     */
    public void setAggregator(final EntryAggregator aggregator) {
        this.aggregator = aggregator;
    }

    @Override
    public void init(final InvocationService invocationservice) {
    }

    @Override
    public void run() {
        String invocationServiceName = ConfigurationFactory.getConfiguraration().getInvocationServiceName();
        MVCCTransactionalCache<K, V> tcache = new MVCCTransactionalCacheImpl<K, V>(
                cacheName.getLogicalName(), invocationServiceName);

        try {
            switch (operation) {
            case get:
                result = tcache.get(transactionId, isolationLevel, key);
                break;
            case put:
                result = tcache.put(transactionId, isolationLevel, autoCommit, key, value);
                break;
            case insert:
                tcache.insert(transactionId, autoCommit, key, value);
                break;
            case invoke:
                result = tcache.invoke(transactionId, isolationLevel, autoCommit, readOnly, key, entryProcessor);
                break;
            case size:
                result = tcache.size(transactionId, isolationLevel);
                break;
            case isEmpty:
                result = tcache.isEmpty(transactionId, isolationLevel);
                break;
            case containsKey:
                result = tcache.containsKey(transactionId, isolationLevel, key);
                break;
            case containsValue:
                result = tcache.containsValue(transactionId, isolationLevel, value);
                break;
            case remove:
                result = tcache.remove(transactionId, isolationLevel, autoCommit, key);
                break;
            case putAll:
                tcache.putAll(transactionId, autoCommit, keyValueMap);
                break;
            case clear:
                tcache.clear(transactionId, autoCommit);
                break;
            case keySet:
                result = tcache.keySet(transactionId, isolationLevel);
                break;
            case values:
                result = tcache.values(transactionId, isolationLevel);
                break;
            case entrySet:
                result = tcache.entrySet(transactionId, isolationLevel);
                break;
            case getAll:
                result = tcache.getAll(transactionId, isolationLevel, colKeys);
                break;
            case entrySetFilter:
                result = tcache.entrySet(transactionId, isolationLevel, filter);
                break;
            case entrySetComparator:
                result = tcache.entrySet(transactionId, isolationLevel, filter, comparator);
                break;
            case keySetFilter:
                result = tcache.keySet(transactionId, isolationLevel, filter);
                break;
            case aggregateKeys:
                result = tcache.aggregate(transactionId, isolationLevel, colKeys, aggregator);
                break;
            case aggregateFilter:
                result = tcache.aggregate(transactionId, isolationLevel, filter, aggregator);
                break;
            case invokeAllKeys:
                result = tcache.invokeAll(transactionId, isolationLevel, autoCommit, readOnly, colKeys, entryProcessor);
                break;
            case invokeAllFilter:
                result = tcache.invokeAll(transactionId, isolationLevel, autoCommit, readOnly, filter, entryProcessor);
                break;
            default:
                throw new IllegalArgumentException("Unexpected operation: " + operation);
            }
        } catch (Throwable e) {
            throw new WrapperException(e);
        }
    }

    @Override
    public Object getResult() {
        return result;
    }

}
