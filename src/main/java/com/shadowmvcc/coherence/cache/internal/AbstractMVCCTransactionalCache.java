package com.shadowmvcc.coherence.cache.internal;

import static com.shadowmvcc.coherence.domain.Constants.LOGICALKEYEXTRACTOR;

import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.cache.MVCCTransactionalCache;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.event.MVCCEventFilter;
import com.shadowmvcc.coherence.event.MVCCEventTransformer;
import com.shadowmvcc.coherence.event.MVCCMapListener;
import com.shadowmvcc.coherence.index.MVCCExtractor;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.CacheService;
import com.tangosol.net.DistributedCacheService;
import com.tangosol.net.NamedCache;
import com.tangosol.net.partition.PartitionSet;
import com.tangosol.util.Filter;
import com.tangosol.util.MapEventTransformer;
import com.tangosol.util.MapListener;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.filter.AlwaysFilter;
import com.tangosol.util.filter.EqualsFilter;
import com.tangosol.util.filter.MapEventTransformerFilter;

/**
 * Abstract base class containing nethods of MVCCTransactionalCache that do not erad
 * or mutate cache contents. i.e. that are applicable to cluster member and extend client
 * implementations.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> cache key type
 * @param <V> cache value type
 */
public abstract class AbstractMVCCTransactionalCache<K, V> implements MVCCTransactionalCache<K, V> {

    protected final NamedCache keyCache;
    protected final NamedCache versionCache;
    protected final CacheName cacheName;

    /**
     * Key class for the local map of MapListeners. Containing the supplied
     * listener and the key or filter parameter used to register (or null for whole cache)
     */
    private static class ListenerMapKey {
        private final MapListener listener;
        private final Object param;
        /**
         * Constructor.
         * @param listener the user's listener
         * @param param the filter or key specifier, or null
         */
        public ListenerMapKey(final MapListener listener, final Object param) {
            super();
            this.listener = listener;
            this.param = param;
        }
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                    + ((listener == null) ? 0 : listener.hashCode());
            result = prime * result + ((param == null) ? 0 : param.hashCode());
            return result;
        }
        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            ListenerMapKey other = (ListenerMapKey) obj;
            if (listener == null) {
                if (other.listener != null) {
                    return false;
                }
            } else if (listener != other.listener) {
                return false;
            }
            if (param == null) {
                if (other.param != null) {
                    return false;
                }
            } else if (!param.equals(other.param)) {
                return false;
            }
            return true;
        }

    }

    /**
     * Constructor.
     * @param strcacheName string form of logical cache name
     */
    public AbstractMVCCTransactionalCache(final String strcacheName) {
        super();
        this.cacheName = new CacheName(strcacheName);
        this.keyCache = CacheFactory.getCache(this.cacheName.getKeyCacheName());
        this.versionCache = CacheFactory.getCache(this.cacheName.getVersionCacheName());
        versionCache.addIndex(MVCCExtractor.INSTANCE, false, null);
    }

    /**
     * Map listener map.
     */
    private final ConcurrentMap<ListenerMapKey, MVCCMapListener<K, V>> listenerMap =
            new ConcurrentHashMap<ListenerMapKey, MVCCMapListener<K, V>>();

    @Override
    public void addMapListener(final MapListener listener,
            final TransactionId tid, final IsolationLevel isolationLevel) {
        MVCCMapListener<K, V> mvccml = new MVCCMapListener<K, V>(listener);
        if (listenerMap.putIfAbsent(new ListenerMapKey(listener, null), mvccml) == null) {
            versionCache.addMapListener(mvccml, 
                    new MapEventTransformerFilter(AlwaysFilter.INSTANCE,
                            new MVCCEventTransformer<K, V>(isolationLevel, tid, cacheName)), false);
        }
    }

    @Override
    public void addMapListener(final MapListener listener, final TransactionId tid, final IsolationLevel isolationLevel,
            final Object oKey, final boolean fLite) {
                Filter keyFilter = new EqualsFilter(LOGICALKEYEXTRACTOR, oKey);
                MVCCMapListener<K, V> mvccml = new MVCCMapListener<K, V>(listener);
                if (listenerMap.putIfAbsent(new ListenerMapKey(listener, oKey), mvccml) == null) {
                    versionCache.addMapListener(mvccml, 
                            new MapEventTransformerFilter(keyFilter,
                                    new MVCCEventTransformer<K, V>(isolationLevel, tid, cacheName)), false);
                }
            }

    @Override
    public void addMapListener(final MapListener listener, final TransactionId tid, final IsolationLevel isolationLevel,
            final Filter filter, final boolean fLite) {
                MVCCMapListener<K, V> mvccml = new MVCCMapListener<K, V>(listener);
                if (listenerMap.putIfAbsent(new ListenerMapKey(listener, filter), mvccml) == null) {
                    MapEventTransformer transformer = new MVCCEventTransformer<K, V>(isolationLevel, tid, cacheName);
                    Filter eventfilter = new MVCCEventFilter<K>(isolationLevel, filter, cacheName, transformer);
                    versionCache.addMapListener(mvccml, eventfilter, false);
                }
            }

    @Override
    public void removeMapListener(final MapListener listener) {
        ListenerMapKey lmk = new ListenerMapKey(listener, null);
        versionCache.removeMapListener(listenerMap.get(lmk));
        listenerMap.remove(lmk);
    }

    @Override
    public void removeMapListener(final MapListener listener, final Object oKey) {
        ListenerMapKey lmk = new ListenerMapKey(listener, oKey);
        versionCache.removeMapListener(listenerMap.get(lmk), oKey);
        listenerMap.remove(lmk);
    }

    @Override
    public void removeMapListener(final MapListener listener, final Filter filter) {
        ListenerMapKey lmk = new ListenerMapKey(listener, filter);
        versionCache.removeMapListener(listenerMap.get(lmk), filter);
        listenerMap.remove(lmk);
    }

    @Override
    public void addIndex(final ValueExtractor extractor, final boolean fOrdered, final Comparator<V> comparator) {
        //TODO index on key extractor not correctly supported
        versionCache.addIndex(extractor, fOrdered, comparator);
    
    }

    @Override
    public void removeIndex(final ValueExtractor extractor) {
        versionCache.removeIndex(extractor);
    }

    @Override
    public void destroy() {
        keyCache.destroy();
        versionCache.destroy();
    }

    @Override
    public String getCacheName() {
        return cacheName.getLogicalName();
    }

    @Override
    public CacheService getCacheService() {
        return versionCache.getCacheService();
    }

    @Override
    public PartitionSet getPartitionSet() {
        PartitionSet partitionSet = new PartitionSet(
                ((DistributedCacheService) versionCache.getCacheService()).getPartitionCount());
        partitionSet.fill();
        return partitionSet;
    }

    @Override
    public boolean isActive() {
        return versionCache.isActive();
    }

    @Override
    public void release() {
        keyCache.release();
        versionCache.release();
    }

    @Override
    public CacheName getMVCCCacheName() {
        return cacheName;
    }

}
