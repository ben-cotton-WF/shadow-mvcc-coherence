package com.sixwhits.cohmvcc.event;

import static com.tangosol.util.MapEvent.ENTRY_DELETED;
import static com.tangosol.util.MapEvent.ENTRY_INSERTED;
import static com.tangosol.util.MapEvent.ENTRY_UPDATED;

import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.TransactionalValue;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.event.MVCCCacheEvent.CommitStatus;
import com.tangosol.io.Serializer;
import com.tangosol.net.cache.CacheEvent;
import com.tangosol.util.Binary;
import com.tangosol.util.ExternalizableHelper;
import com.tangosol.util.MapEvent;
import com.tangosol.util.MapListener;

/**
 * Wrapper for a user provided {@link MapListener}. Translates version cache events into logical cache
 * events according to the users data model. Events must have been previously transformed by {@link MVCCEvebtTransformer}
 * to populate the correct earlier version and optionally suppress uncommitted events.
 *
 * The distinction between insert and update is somewhat mutable in MVCC. An event reported as an insert may
 * be retrospectively changed to an update if an earlier (ts-time) version is later (real-time) added.
 * 
 * Events propagated to the client will be instances of {@link MVCCMapEvent} or {@link MVCCCacheEvent} so that commit 
 * status and invoking transaction id can be made available to the client. 
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class MVCCMapListener<K,V> implements MapListener {
	
	private final MapListener delegate;
	private final Serializer serializer;

	public MVCCMapListener(MapListener delegate, Serializer serializer) {
		super();
		this.delegate = delegate;
		this.serializer = serializer;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * "Inserted" in the versioned cache may represent an insert, update, or delete in the logical cache.
	 * As noted above, the distinction between insert and update is semantically dubious for MVCC
	 */
	@Override
	public void entryInserted(MapEvent mapevent) {
		
		TransactionId eventTransactionId = extractTransactionId(mapevent);

		boolean synthetic = false;
		if (mapevent instanceof CacheEvent) {
			synthetic = ((CacheEvent)mapevent).isSynthetic();
		}
		
		int eventType = ((TransactionalValue)mapevent.getNewValue()).isDeleted() ? ENTRY_DELETED : mapevent.getOldValue() == null ? ENTRY_INSERTED : ENTRY_UPDATED;
		
		MVCCCacheEvent newEvent = new MVCCCacheEvent(
				mapevent.getMap(), eventType, extractKey(mapevent),
				extractValue((TransactionalValue) mapevent.getOldValue()),
				extractValue((TransactionalValue) mapevent.getNewValue()),
				synthetic, eventTransactionId,
				isCommitted(mapevent) ? CommitStatus.commit : CommitStatus.open);
		
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
		}
	}

	/**
	 * {@inheritDoc}
	 * 
	 * "Updated" in the versioned cache can only represent an entry being committed, but will be processed
	 * identically to an insert.
	 */
	@Override
	public void entryUpdated(MapEvent mapevent) {
		entryInserted(mapevent);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * "Deleted" in the versioned cache represents a rollback, or version reaping event, which we can identify as the old
	 * version will have been committed. This will be reported as an update, with null new version
	 */
	@Override
	public void entryDeleted(MapEvent mapevent) {

		boolean synthetic = false;
		if (mapevent instanceof CacheEvent) {
			synthetic = ((CacheEvent)mapevent).isSynthetic();
		}
		
		MVCCCacheEvent newEvent = new MVCCCacheEvent(
				mapevent.getMap(), ENTRY_UPDATED, extractKey(mapevent),
				extractValue((TransactionalValue) mapevent.getOldValue()), null, synthetic,
				extractTransactionId(mapevent),
				CommitStatus.rollback);
		delegate.entryUpdated(newEvent);
	}
	
	/**
	 * Extract the logical key from the raw event
	 * @param rawEvent event from the versioned cache
	 * @return the logical key
	 */
	private K extractKey(MapEvent rawEvent) {
		@SuppressWarnings("unchecked")
		VersionedKey<K> vk = (VersionedKey<K>) rawEvent.getKey();
		return vk.getNativeKey();
	}
	
	private TransactionId extractTransactionId(MapEvent rawEvent) {
		@SuppressWarnings("unchecked")
		VersionedKey<K> vk = (VersionedKey<K>) rawEvent.getKey();
		return vk.getTxTimeStamp();
	}
	
	private V extractValue(TransactionalValue tv) {
		Binary binaryValue = tv.getValue();
		@SuppressWarnings("unchecked")
		V value = (V) ExternalizableHelper.fromBinary(binaryValue, serializer);
		return value;
	}
	
	private boolean isCommitted(MapEvent rawEvent) {
		TransactionalValue tv = (TransactionalValue) rawEvent.getNewValue();
		return tv.isCommitted();
	}

}
