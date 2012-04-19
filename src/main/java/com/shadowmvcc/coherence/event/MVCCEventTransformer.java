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

import java.util.Map;
import java.util.Map.Entry;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.domain.EventValue;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.Utils;
import com.shadowmvcc.coherence.domain.VersionedKey;
import com.shadowmvcc.coherence.index.MVCCExtractor;
import com.shadowmvcc.coherence.index.MVCCIndex;
import com.shadowmvcc.coherence.index.MVCCIndex.IndexEntry;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.net.BackingMapContext;
import com.tangosol.net.BackingMapManagerContext;
import com.tangosol.net.cache.CacheEvent;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ExternalizableHelper;
import com.tangosol.util.MapEvent;
import com.tangosol.util.MapEventTransformer;

/**
 * Transform MapListener events on the version cache so that oldValue
 * reflects the previous version value in timestamp order. Optionally suppresses
 * uncommitted events, out of sequence (timestamp ordering) events, and events
 * backdated beyond a starting threshold.
 *
 * Old and new values represent the physical model of the version cache, {@link MVCCMapListener}
 * takes care of mapping to the logical data model and convert event type (event id in Coherence parlance).
 *
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> key type for the logical cache
 * @param <V> value type for the cache
 */
@Portable
public class MVCCEventTransformer<K, V> implements MapEventTransformer {

    public static final int POF_ISOLATION = 0;
    @PortableProperty(POF_ISOLATION)
    private IsolationLevel isolationLevel;

    public static final int POF_TX = 1;
    @PortableProperty(POF_TX)
    private TransactionId initialTransactionId;

    public static final int POF_NAME = 2;
    @PortableProperty(POF_NAME)
    private CacheName cacheName;

    public static final int POF_LATEST = 3;
    @PortableProperty(POF_LATEST)
    private boolean latestOnly = true;

    /**
     * Default constructor for POF use only.
     */
    public MVCCEventTransformer() {
        super();
    }

    /**
     * Constructor.
     * @param isolationLevel isolation level. Uncommitted events will be ignore unless this is {@code readUncommitted}
     * @param initialTransactionId ignore events pertaining to changes earlier than the initial id
     * @param cacheName the cache name
     */
    public MVCCEventTransformer(final IsolationLevel isolationLevel, 
            final TransactionId initialTransactionId, final CacheName cacheName) {
        super();
        this.isolationLevel = isolationLevel;
        this.initialTransactionId = initialTransactionId;
        this.cacheName = cacheName;
    }

    /**
     * Constructor.
     * @param isolationLevel isolation level. Uncommitted events will be ignore unless this is {@code readUncommitted}
     * @param initialTransactionId ignore events pertaining to changes earlier than the initial id
     * @param cacheName the cache name
     * @param latestOnly if true, ignore events for any version other than the newest
     */
    public MVCCEventTransformer(final IsolationLevel isolationLevel, 
            final TransactionId initialTransactionId, final CacheName cacheName, 
            final boolean latestOnly) {
        super();
        this.isolationLevel = isolationLevel;
        this.initialTransactionId = initialTransactionId;
        this.cacheName = cacheName;
        this.latestOnly = latestOnly;
    }

    @SuppressWarnings("unchecked")
    @Override
    public MapEvent transform(final MapEvent mapevent) {
        boolean committed = isCommitted(mapevent);
        if (isolationLevel != readUncommitted && !committed) {
            return null;
        }

        TransactionId eventTransactionId = extractTransactionId(mapevent);
        if (initialTransactionId != null && eventTransactionId.compareTo(initialTransactionId) < 0) {
            return null;
        }

        BinaryEntry currentEntry = (BinaryEntry) mapevent.getNewEntry();

        BackingMapManagerContext mctx = currentEntry.getContext();
        BackingMapContext ctx = mctx.getBackingMapContext(cacheName.getVersionCacheName());
        @SuppressWarnings("rawtypes")
        Map indexMap = ctx.getIndexMap();
        MVCCIndex<K> index = (MVCCIndex<K>) indexMap.get(MVCCExtractor.INSTANCE);
        VersionedKey<K> currentVersion = (VersionedKey<K>) mapevent.getKey();

        if (latestOnly) {
            Entry<TransactionId, IndexEntry> ixe =
                    index.higherEntry(currentVersion.getNativeKey(), currentVersion.getTimeStamp());
            while (ixe != null) {
                if (ixe.getValue().isCommitted() || isolationLevel == readUncommitted) {
                    return null;
                }
                ixe = index.higherEntry(currentVersion.getNativeKey(), ixe.getKey());
            }
        }

        Object oldValue = null;

        EventValue<V> eventValue = null;

        if (mapevent.getId() == MapEvent.ENTRY_DELETED) {
            if (wasCommitted(mapevent)) {
                return null;
            }

            oldValue = mapevent.getOldValue();
        } else {

            Entry<TransactionId, IndexEntry> ixe = 
                    index.lowerEntry(currentVersion.getNativeKey(), currentVersion.getTimeStamp());
            while (ixe != null
                    && !ixe.getValue().isCommitted()
                    && isolationLevel != readUncommitted) {
                ixe = index.lowerEntry(currentVersion.getNativeKey(), ixe.getKey());
            }

            if (ixe != null) {
                Binary priorBinaryKey = ixe.getValue().getBinaryKey();
                @SuppressWarnings("rawtypes")
                Map backingMap = ctx.getBackingMap();
                Binary priorBinaryValue = (Binary) backingMap.get(priorBinaryKey);
                oldValue = ExternalizableHelper.fromBinary(priorBinaryValue, currentEntry.getSerializer());
            }
            boolean deleted = isDeleted(mapevent);
            eventValue = new EventValue<V>(committed, deleted, deleted ? null : (V) mapevent.getNewValue());
        }

        if (mapevent instanceof CacheEvent) {
            return new CacheEvent(
                    mapevent.getMap(), mapevent.getId(), mapevent.getKey(), 
                    oldValue, eventValue, ((CacheEvent) mapevent).isSynthetic());
        } else {
            return new MapEvent(
                    mapevent.getMap(), mapevent.getId(), mapevent.getKey(), 
                    oldValue, eventValue);
        }
    }

    /**
     * Get the incoming event's transaction id.
     * @param rawEvent the incoming event
     * @return the transaction id
     */
    private TransactionId extractTransactionId(final MapEvent rawEvent) {
        @SuppressWarnings("unchecked")
        VersionedKey<K> vk = (VersionedKey<K>) rawEvent.getKey();
        return vk.getTimeStamp();
    }

    /**
     * Is the event on a committed version?
     * @param rawEvent the incoming event
     * @return true if committed
     */
    private boolean isCommitted(final MapEvent rawEvent) {
        return Utils.isCommitted((BinaryEntry) rawEvent.getNewEntry());
    }

    /**
     * Is the event on a version marked as a delete operation?
     * @param rawEvent the incoming event
     * @return true if the version is a delete
     */
    private boolean isDeleted(final MapEvent rawEvent) {
        return Utils.isDeleted((BinaryEntry) rawEvent.getNewEntry());
    }

    /**
     * For an event representing a physical delete of a version, was that version committed?
     * @param rawEvent the incoming event
     * @return true if the version was committed
     */
    private boolean wasCommitted(final MapEvent rawEvent) {
        return Utils.isCommitted((BinaryEntry) rawEvent.getOldEntry());
    }
}
