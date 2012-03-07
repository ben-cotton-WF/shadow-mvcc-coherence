package com.sixwhits.cohmvcc.event;

import static com.sixwhits.cohmvcc.domain.IsolationLevel.readUncommitted;

import java.util.Map;
import java.util.Map.Entry;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.TransactionalValue;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.index.MVCCExtractor;
import com.sixwhits.cohmvcc.index.MVCCIndex;
import com.sixwhits.cohmvcc.index.MVCCIndex.IndexEntry;
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
 */
@Portable
public class MVCCEventTransformer<K> implements MapEventTransformer {

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
	
	public MVCCEventTransformer() {
		super();
	}

	public MVCCEventTransformer(IsolationLevel isolationLevel,
			TransactionId initialTransactionId, CacheName cacheName) {
		super();
		this.isolationLevel = isolationLevel;
		this.initialTransactionId = initialTransactionId;
		this.cacheName = cacheName;
	}
	
	public MVCCEventTransformer(IsolationLevel isolationLevel,
			TransactionId initialTransactionId, CacheName cacheName,
			boolean latestOnly) {
		super();
		this.isolationLevel = isolationLevel;
		this.initialTransactionId = initialTransactionId;
		this.cacheName = cacheName;
		this.latestOnly = latestOnly;
	}

	@Override
	public MapEvent transform(MapEvent mapevent) {
		if (isolationLevel != readUncommitted && !isCommitted(mapevent)) {
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
		@SuppressWarnings("unchecked")
		MVCCIndex<K> index = (MVCCIndex<K>) indexMap.get(MVCCExtractor.INSTANCE);
		@SuppressWarnings("unchecked")
		VersionedKey<K> currentVersion = (VersionedKey<K>) mapevent.getKey();
		
		if (latestOnly) {
			Entry<TransactionId, IndexEntry> ixe = index.higherEntry(currentVersion.getNativeKey(), currentVersion.getTxTimeStamp());
			while (ixe != null) { 
				if (ixe.getValue().isCommitted() || isolationLevel == readUncommitted) {
					return null;
				}
				ixe = index.higherEntry(currentVersion.getNativeKey(), ixe.getKey());
			}
		}
		
		TransactionalValue oldValue = null;

		if (mapevent.getId() == MapEvent.ENTRY_DELETED) {
			if (wasCommitted(mapevent)) {
				return null;
			}
			
			oldValue = (TransactionalValue) mapevent.getOldValue();
		} else {

			Entry<TransactionId, IndexEntry> ixe = index.lowerEntry(currentVersion.getNativeKey(), currentVersion.getTxTimeStamp());
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
				oldValue = (TransactionalValue) ExternalizableHelper.fromBinary(priorBinaryValue, currentEntry.getSerializer());
			}
		}
		if (mapevent instanceof CacheEvent) {
			return new CacheEvent(
					mapevent.getMap(), mapevent.getId(), mapevent.getKey(),
					oldValue, mapevent.getNewValue(), ((CacheEvent)mapevent).isSynthetic());
		} else {
			return new MapEvent(
					mapevent.getMap(), mapevent.getId(), mapevent.getKey(),
					oldValue, mapevent.getNewValue());
		}
	}

	private TransactionId extractTransactionId(MapEvent rawEvent) {
		@SuppressWarnings("unchecked")
		VersionedKey<K> vk = (VersionedKey<K>) rawEvent.getKey();
		return vk.getTxTimeStamp();
	}

	private boolean isCommitted(MapEvent rawEvent) {
		TransactionalValue tv = (TransactionalValue) rawEvent.getNewValue();
		return tv.isCommitted();
	}

	private boolean wasCommitted(MapEvent rawEvent) {
		TransactionalValue tv = (TransactionalValue) rawEvent.getOldValue();
		return tv.isCommitted();
	}
}
