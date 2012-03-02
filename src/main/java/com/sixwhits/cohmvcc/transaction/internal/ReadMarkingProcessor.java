package com.sixwhits.cohmvcc.transaction.internal;

import static com.sixwhits.cohmvcc.domain.IsolationLevel.readCommitted;
import static com.sixwhits.cohmvcc.domain.IsolationLevel.readUncommitted;
import static com.sixwhits.cohmvcc.domain.IsolationLevel.repeatableRead;
import static com.sixwhits.cohmvcc.domain.IsolationLevel.serializable;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.Constants;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.ProcessorResult;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.invocable.AbstractMVCCProcessor;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.InvocableMap.Entry;

@Portable
public class ReadMarkingProcessor<K> extends AbstractMVCCProcessor<K,VersionedKey<K>> {

	private static final long serialVersionUID = -6559372127281694088L;

	public static final int POF_RETURNKEYS = 0;
	@PortableProperty(POF_RETURNKEYS)
	private boolean returnMatchingKeys = false;
	public ReadMarkingProcessor() {
		super();
	}

	public ReadMarkingProcessor(TransactionId transactionId,
			IsolationLevel isolationLevel, CacheName cacheName) {
		super(transactionId, isolationLevel, cacheName);
	}

	public ReadMarkingProcessor(TransactionId transactionId,
			IsolationLevel isolationLevel, CacheName cacheName,
			boolean returnMatchingKeys) {
		super(transactionId, isolationLevel, cacheName);
		this.returnMatchingKeys = returnMatchingKeys;
	}

	@SuppressWarnings("unchecked")
	@Override
	public ProcessorResult<K,VersionedKey<K>> process(Entry arg) {
		BinaryEntry entry = (BinaryEntry) arg;
		Binary priorVersionBinaryKey = getPriorVersionBinaryKey(entry);
		if (priorVersionBinaryKey == null) {
			return null;
		}

		BinaryEntry priorEntry = (BinaryEntry) getVersionCacheBackingMapContext(entry).getBackingMapEntry(priorVersionBinaryKey);

		if (isolationLevel != readUncommitted) {
			boolean committed = (Boolean) Constants.COMMITSTATUSEXTRACTOR.extractFromEntry(priorEntry);
			if (!committed) {
				return new ProcessorResult<K,VersionedKey<K>>(null, (VersionedKey<K>)priorEntry.getKey());
			}
		}

		boolean deleted = (Boolean) Constants.DELETESTATUSEXTRACTOR.extractFromEntry(priorEntry);
		if (deleted) {
			return null;
		}

		if (isolationLevel == repeatableRead || isolationLevel == serializable) {
			setReadTimestamp(entry);
		}
		
		return returnMatchingKeys ? new ProcessorResult<K, VersionedKey<K>>((VersionedKey<K>)priorEntry.getKey(), null) : null;
	}

	public IsolationLevel getIsolationLevel() {
		return isolationLevel;
	}

}
