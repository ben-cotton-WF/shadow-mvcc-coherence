package com.sixwhits.cohmvcc.event;

import static com.tangosol.util.MapEvent.ENTRY_DELETED;
import static com.tangosol.util.MapEvent.ENTRY_INSERTED;
import static com.tangosol.util.MapEvent.ENTRY_UPDATED;

import com.sixwhits.cohmvcc.domain.EventValue;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.event.MVCCCacheEvent.CommitStatus;
import com.tangosol.net.cache.CacheEvent;
import com.tangosol.util.MapEvent;
import com.tangosol.util.MapListener;

/**
 * Wrapper for a user provided {@link MapListener}. Translates version cache events into logical cache
 * events according to the users data model. Events must have been previously transformed
 * by {@link MVCCEventTransformer}
 * to populate the correct earlier version and optionally suppress uncommitted events.
 *
 * The distinction between insert and update is somewhat mutable in MVCC. An event reported as an insert may
 * be retrospectively changed to an update if an earlier (ts-time) version is later (real-time) added.
 *
 * Events propagated to the client will be instances of {@link MVCCCacheEvent} so that commit
 * status and invoking transaction id can be made available to the client.
 *
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> cache key type
 * @param <V> cache version type
 */
public class MVCCMapListener<K, V> implements MapListener {

    private final MapListener delegate;

    /**
     * Constructor.
     * @param delegate map listener to received logical cache events
     */
    public MVCCMapListener(final MapListener delegate) {
        super();
        this.delegate = delegate;
    }

    /**
     * {@inheritDoc}
     *
     * "Inserted" in the versioned cache may represent an insert, update, or delete in the logical cache.
     * As noted above, the distinction between insert and update is semantically dubious for MVCC
     */
    @Override
    public void entryInserted(final MapEvent mapevent) {

        TransactionId eventTransactionId = extractTransactionId(mapevent);

        boolean synthetic = false;
        if (mapevent instanceof CacheEvent) {
            synthetic = ((CacheEvent) mapevent).isSynthetic();
        }

        @SuppressWarnings("unchecked")
        EventValue<V> eventValue = (EventValue<V>) mapevent.getNewValue();
        int eventType = eventValue.isDeleted() ? ENTRY_DELETED
                : mapevent.getOldValue() == null ? ENTRY_INSERTED : ENTRY_UPDATED;

        MVCCCacheEvent newEvent = new MVCCCacheEvent(
                mapevent.getMap(), eventType, extractKey(mapevent), 
                mapevent.getOldValue(), 
                eventValue.getValue(), 
                synthetic, eventTransactionId, 
                eventValue.isCommitted() ? CommitStatus.commit : CommitStatus.open);

        switch (eventType) {
        case ENTRY_DELETED:
            delegate.entryDeleted(newEvent);
            break;
        case ENTRY_INSERTED:
            delegate.entryInserted(newEvent);
            break;
        case ENTRY_UPDATED:
            delegate.entryUpdated(newEvent);
            break;
        default:
            throw new IllegalArgumentException("event received with illegal event type " + mapevent);
        }
    }

    /**
     * {@inheritDoc}
     *
     * "Updated" in the versioned cache can only represent an entry being committed, but will be processed
     * identically to an insert.
     */
    @Override
    public void entryUpdated(final MapEvent mapevent) {
        entryInserted(mapevent);
    }

    /**
     * {@inheritDoc}
     *
     * "Deleted" in the versioned cache represents a rollback. This will be reported as an update, with null new version
     */
    @Override
    public void entryDeleted(final MapEvent mapevent) {

        boolean synthetic = false;
        if (mapevent instanceof CacheEvent) {
            synthetic = ((CacheEvent) mapevent).isSynthetic();
        }

        @SuppressWarnings("unchecked")
        EventValue<V> eventValue = (EventValue<V>) mapevent.getNewValue();

        MVCCCacheEvent newEvent = new MVCCCacheEvent(
                mapevent.getMap(), ENTRY_UPDATED, extractKey(mapevent), 
                mapevent.getOldValue(), 
                eventValue, 
                synthetic, extractTransactionId(mapevent), 
                CommitStatus.rollback);
        delegate.entryUpdated(newEvent);
    }

    /**
     * Extract the logical key from the raw event.
     * @param rawEvent event from the versioned cache
     * @return the logical key
     */
    private K extractKey(final MapEvent rawEvent) {
        @SuppressWarnings("unchecked")
        VersionedKey<K> vk = (VersionedKey<K>) rawEvent.getKey();
        return vk.getNativeKey();
    }

    /**
     * @param rawEvent the event received
     * @return the transaction id of the event
     */
    private TransactionId extractTransactionId(final MapEvent rawEvent) {
        @SuppressWarnings("unchecked")
        VersionedKey<K> vk = (VersionedKey<K>) rawEvent.getKey();
        return vk.getTxTimeStamp();
    }

}
