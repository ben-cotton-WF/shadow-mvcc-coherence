package com.sixwhits.cohmvcc.event;

import static com.sixwhits.cohmvcc.domain.IsolationLevel.readUncommitted;

import java.util.Map.Entry;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.index.MVCCExtractor;
import com.sixwhits.cohmvcc.index.MVCCIndex;
import com.sixwhits.cohmvcc.index.MVCCIndex.IndexEntry;
import com.tangosol.io.Serializer;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.net.BackingMapContext;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.DistributedCacheService;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ExternalizableHelper;
import com.tangosol.util.Filter;
import com.tangosol.util.MapEvent;
import com.tangosol.util.MapEventTransformer;
import com.tangosol.util.ObservableMap;
import com.tangosol.util.filter.EntryFilter;

/**
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * Used to evaluate a filter in the context of a MapListener registration by filter, where
 * it is necessary to check both present and previous (in timestamp ordering) versions for a match
 * in order to detect changes causing an entry to leave a result set.
 *
 * @param <K> The key type of the logical cache
 */
@Portable
public class MVCCEventFilter<K> implements EntryFilter, MapEventTransformer {

    @PortableProperty(0) private IsolationLevel isolationLevel;
    @PortableProperty(1) private Filter delegate;
    @PortableProperty(2) private CacheName cacheName;
    @PortableProperty(3) private MapEventTransformer transformer;
    
    /**
     *  Default constructor for POF use only.
     */
    public MVCCEventFilter() {
        super();
    }

    /**
     * Constructor.
     * @param isolationLevel isolation level
     * @param delegate filter to delegate evaluation to
     * @param cacheName the cache name
     * @param transformer event transformer to delegate to
     */
    public MVCCEventFilter(final IsolationLevel isolationLevel, final Filter delegate,
            final CacheName cacheName, final MapEventTransformer transformer) {
        super();
        this.isolationLevel = isolationLevel;
        this.delegate = delegate;
        this.cacheName = cacheName;
        this.transformer = transformer;
    }

    @Override
    public boolean evaluate(final Object obj) {
        return true;
    }

    @Override
    public boolean evaluateEntry(@SuppressWarnings("rawtypes") final Entry arg) {

        BinaryEntry entry = (BinaryEntry) arg;

        boolean currentVersionMatch = match(entry);

        if (currentVersionMatch) {
            return true;
        }

        @SuppressWarnings("unchecked")
        MVCCIndex<K> index = (MVCCIndex<K>) entry.getBackingMapContext().getIndexMap().get(MVCCExtractor.INSTANCE);
        @SuppressWarnings("unchecked")
        VersionedKey<K> currentVersion = (VersionedKey<K>) entry.getKey();

        Entry<TransactionId, IndexEntry> ixe = index.lowerEntry(
                currentVersion.getNativeKey(), currentVersion.getTxTimeStamp());
        while (ixe != null && !(isolationLevel == readUncommitted || ixe.getValue().isCommitted())) {
            ixe = index.lowerEntry(currentVersion.getNativeKey(), ixe.getKey());
        }

        if (ixe != null) {
            Binary priorBinaryKey = ixe.getValue().getBinaryKey();
            Binary priorBinaryValue = (Binary) entry.getBackingMap().get(priorBinaryKey);

            @SuppressWarnings("unchecked")
            VersionedKey<K> priorKey = (VersionedKey<K>) ExternalizableHelper.fromBinary(
                    priorBinaryKey, entry.getSerializer());
            Binary logicalBinaryKey = ExternalizableHelper.toBinary(priorKey.getNativeKey());

            @SuppressWarnings("rawtypes")
            BinaryEntry priorEntry = new SyntheticBinaryEntry(logicalBinaryKey, priorBinaryValue, 
                    entry.getSerializer(), entry.getBackingMapContext());

            return match(priorEntry);

        }

        return false;

    }

    /**
     * Check if the entry matches the filter.
     * @param entry the entry
     * @return true if it matches
     */
    private boolean match(final BinaryEntry entry) {
        if (delegate instanceof EntryFilter) {
            return ((EntryFilter) delegate).evaluateEntry(entry);
        } else {
            return delegate.evaluate(entry.getValue());
        }
    }

    @Override
    public MapEvent transform(final MapEvent mapevent) {
        
        DistributedCacheService cacheService = (DistributedCacheService) CacheFactory.getCache(
                cacheName.getVersionCacheName()).getCacheService();
        
        Serializer serializer = cacheService.getSerializer();
        
        BackingMapContext bmc = cacheService.getBackingMapManager().getContext().
                getBackingMapContext(cacheName.getVersionCacheName());
        ObservableMap map = bmc.getBackingMap();
        
        Binary binaryKey = (Binary) bmc.getManagerContext().getKeyToInternalConverter().convert(mapevent.getKey());
        Binary binaryValue = (Binary) map.get(binaryKey);
        
        @SuppressWarnings("rawtypes")
        SyntheticBinaryEntry entry = new SyntheticBinaryEntry(binaryKey, binaryValue, serializer, bmc);
        
        if (evaluateEntry(entry)) {
            return transformer.transform(mapevent);
        } else {
            return null;
        }
    }
}
