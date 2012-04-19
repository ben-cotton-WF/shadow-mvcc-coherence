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

package com.shadowmvcc.coherence.event;

import static com.shadowmvcc.coherence.domain.IsolationLevel.readUncommitted;

import java.util.Map.Entry;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.VersionedKey;
import com.shadowmvcc.coherence.index.MVCCExtractor;
import com.shadowmvcc.coherence.index.MVCCIndex;
import com.shadowmvcc.coherence.index.MVCCIndex.IndexEntry;
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
                currentVersion.getLogicalKey(), currentVersion.getTimeStamp());
        while (ixe != null && !(isolationLevel == readUncommitted || ixe.getValue().isCommitted())) {
            ixe = index.lowerEntry(currentVersion.getLogicalKey(), ixe.getKey());
        }

        if (ixe != null) {
            Binary priorBinaryKey = ixe.getValue().getBinaryKey();
            Binary priorBinaryValue = (Binary) entry.getBackingMap().get(priorBinaryKey);

            @SuppressWarnings("unchecked")
            VersionedKey<K> priorKey = (VersionedKey<K>) ExternalizableHelper.fromBinary(
                    priorBinaryKey, entry.getSerializer());
            Binary logicalBinaryKey = ExternalizableHelper.toBinary(priorKey.getLogicalKey());

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
