package com.shadowmvcc.coherence.invocable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.tangosol.net.BackingMapContext;
import com.tangosol.net.BackingMapManager;
import com.tangosol.net.BackingMapManagerContext;
import com.tangosol.net.CacheService;
import com.tangosol.run.xml.XmlElement;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.Converter;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.MapIndex;
import com.tangosol.util.ObservableMap;
import com.tangosol.util.ValueExtractor;

/**
 * Wrapper around a {@link BackingMapManagerContext} to provide wrappers around
 * references to MVCC cache backing maps.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class MVCCBackingMapManagerContext implements BackingMapManagerContext {
    
    /**
     * Wrapper round the BackingMapContext for an MVCC cache.
     */
    public final class MVCCBackingMapContext implements BackingMapContext {
        private final CacheName cacheName;
        private Map<Object, ReadWriteEntryWrapper> wrappedEntries;

        /**
         * Constructor.
         * @param s logical names of the cache
         */
        private MVCCBackingMapContext(final String s) {
            this.cacheName = new CacheName(s);
        }

        @Override
        public BackingMapManagerContext getManagerContext() {
            return MVCCBackingMapManagerContext.this;
        }

        @Override
        public Map<ValueExtractor, MapIndex> getIndexMap() {
            return parentContext.getBackingMapContext(cacheName.getVersionCacheName()).getIndexMap();
        }

        @Override
        public String getCacheName() {
            return cacheName.getLogicalName();
        }

        @Override
        public ReadWriteEntryWrapper getBackingMapEntry(final Object obj) {
            BinaryEntry parentEntry = (BinaryEntry) parentContext.getBackingMapContext(
                    cacheName.getKeyCacheName()).getBackingMapEntry(obj);

            if (wrappedEntries == null) {
                wrappedEntries = new HashMap<Object, ReadWriteEntryWrapper>();
            }
            if (!wrappedEntries.containsKey(obj)) {
                wrappedEntries.put(obj, new ReadWriteEntryWrapper(
                        parentEntry, transactionId, isolationLevel, cacheName,
                        MVCCBackingMapManagerContext.this));
            }
            return wrappedEntries.get(obj);
        }
        
        @Override
        public ObservableMap getBackingMap() {
            throw new UnsupportedOperationException(
                    "operation not permitted on MVCC cache " + cacheName.getLogicalName());
        }
        
        /**
         * Get the collection of wrapped entries that have been obtained from the context.
         * @return the collection of wrapped entries
         */
        @SuppressWarnings("unchecked")
        public Collection<ReadWriteEntryWrapper> getWrappedEntries() {
            return wrappedEntries == null ? Collections.EMPTY_LIST : wrappedEntries.values();
        }
    }

    private final BackingMapManagerContext parentContext;
    private final Map<String, MVCCBackingMapContext> mvccBackingMapContexts;
    private final Map<String, BackingMapContext> otherBackingMapContexts;
    private final TransactionId transactionId;
    private final IsolationLevel isolationLevel;
    private final BackingMapManager backingMapManager;

    /**
     * Constructor.
     * @param parentContext the parent cache
     * @param mvccCacheNames names of MVCC caches that should be wrapped
     * @param transactionId the transaction id
     * @param isolationLevel isolation level
     */
    public MVCCBackingMapManagerContext(final BackingMapManagerContext parentContext,
            final Collection<CacheName> mvccCacheNames,
            final TransactionId transactionId,
            final IsolationLevel isolationLevel) {
        super();
        this.parentContext = parentContext;
        this.mvccBackingMapContexts = new HashMap<String, MVCCBackingMapContext>();
        for (CacheName cacheName : mvccCacheNames) {
            mvccBackingMapContexts.put(cacheName.getLogicalName(), null);
        }
        this.otherBackingMapContexts = new HashMap<String, BackingMapContext>();
        this.transactionId = transactionId;
        this.isolationLevel = isolationLevel;
        this.backingMapManager = new BackingMapManager() {
            
            @Override
            public void releaseBackingMap(final String s, @SuppressWarnings("rawtypes") final Map map) {
                if (isMVCCCache(s)) {
                    throw new UnsupportedOperationException("operation not permitted on MVCC cache " + s);
                }
                parentContext.getManager().releaseBackingMap(s, map);
            }
            
            @SuppressWarnings("rawtypes")
            @Override
            public Map instantiateBackingMap(final String s) {
                if (isMVCCCache(s)) {
                    throw new UnsupportedOperationException("operation not permitted on MVCC cache " + s);
                }
                return parentContext.getManager().instantiateBackingMap(s);
            }
            
            @Override
            public void init(final BackingMapManagerContext backingmapmanagercontext) {
                parentContext.getManager().init(backingmapmanagercontext);
            }
            
            @Override
            public BackingMapManagerContext getContext() {
                return MVCCBackingMapManagerContext.this;
            }
        };
    }

    @Override
    public BackingMapManager getManager() {
        return backingMapManager;
    }

    @Override
    public CacheService getCacheService() {
        return parentContext.getCacheService();
    }

    @Override
    public ClassLoader getClassLoader() {
        return parentContext.getClassLoader();
    }

    @Override
    public void setClassLoader(final ClassLoader classloader) {
        parentContext.setClassLoader(classloader);
    }

    @Override
    public Converter getKeyToInternalConverter() {
        return parentContext.getKeyToInternalConverter();
    }

    @Override
    public Converter getKeyFromInternalConverter() {
        return parentContext.getKeyFromInternalConverter();
    }

    @Override
    public Converter getValueToInternalConverter() {
        return parentContext.getValueToInternalConverter();
    }

    @Override
    public Converter getValueFromInternalConverter() {
        return parentContext.getValueFromInternalConverter();
    }

    @Override
    public boolean isKeyOwned(final Object obj) {
        return parentContext.isKeyOwned(obj);
    }

    @Override
    public int getKeyPartition(final Object obj) {
        return parentContext.getKeyPartition(obj);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Set getPartitionKeys(final String s, final int i) {
        CacheName mvccCacheName = findMVCCCacheName(s);
        return parentContext.getPartitionKeys(mvccCacheName == null ? s : mvccCacheName.getKeyCacheName(), i);
    }

    @SuppressWarnings({ "rawtypes", "deprecation" })
    @Override
    public Map getBackingMap(final String s) {
        if (isMVCCCache(s)) {
            throw new UnsupportedOperationException("operation not permitted on MVCC cache " + s);
        }
        return parentContext.getBackingMap(s);
    }

    @Override
    public BackingMapContext getBackingMapContext(final String s) {
        
        if (mvccBackingMapContexts.containsKey(s)) {
            if (mvccBackingMapContexts.get(s) == null) {
                mvccBackingMapContexts.put(s, new MVCCBackingMapContext(s));
            }
            return mvccBackingMapContexts.get(s);
        } else {
            if (otherBackingMapContexts.get(s) == null) {
                otherBackingMapContexts.put(s, new BackingMapContext() {
                    
                    @Override
                    public BackingMapManagerContext getManagerContext() {
                        return MVCCBackingMapManagerContext.this;
                    }
                    
                    @Override
                    public Map<ValueExtractor, MapIndex> getIndexMap() {
                        return parentContext.getBackingMapContext(s).getIndexMap();
                    }
                    
                    @Override
                    public String getCacheName() {
                        return s;
                    }
                    
                    @Override
                    public Entry getBackingMapEntry(final Object obj) {
                        return parentContext.getBackingMapContext(s).getBackingMapEntry(obj);
                    }
                    
                    @Override
                    public ObservableMap getBackingMap() {
                        return parentContext.getBackingMapContext(s).getBackingMap();
                    }
                });
            }
            return otherBackingMapContexts.get(s);
        }
    }

    @Override
    public Object addInternalValueDecoration(final Object obj, final int i, final Object obj1) {
        return parentContext.addInternalValueDecoration(obj, i, obj1);
    }

    @Override
    public Object removeInternalValueDecoration(final Object obj, final int i) {
        return parentContext.removeInternalValueDecoration(obj, i);
    }

    @Override
    public boolean isInternalValueDecorated(final Object obj, final int i) {
        return parentContext.isInternalValueDecorated(obj, i);
    }

    @Override
    public Object getInternalValueDecoration(final Object obj, final int i) {
        return parentContext.getInternalValueDecoration(obj, i);
    }

    @Override
    public XmlElement getConfig() {
        return parentContext.getConfig();
    }

    @Override
    public void setConfig(final XmlElement xmlelement) {
        parentContext.setConfig(xmlelement);
    }
    
    /**
     * Check if the cache is a known MVCC cache.
     * @param s cache name
     * @return true if this is an MVCC cache
     */
    private boolean isMVCCCache(final String s) {
        return mvccBackingMapContexts.containsKey(s);
    }
    
    /**
     * Get the CacheName object if this is an MVCC cache.
     * @param s the string cache name
     * @return the CacheName object
     */
    private CacheName findMVCCCacheName(final String s) {
        if (isMVCCCache(s)) {
            return new CacheName(s);
        }
        return null;
    }
    
    /**
     * Get the collection of MVCC backing map contexts that have been referenced through this manager context.
     * @return the collection of MVCC backing map contexts
     */
    @SuppressWarnings("unchecked")
    public Collection<MVCCBackingMapContext> getMVCCBackingMapContexts() {
        return mvccBackingMapContexts == null ? Collections.EMPTY_LIST : mvccBackingMapContexts.values();
    }

}
