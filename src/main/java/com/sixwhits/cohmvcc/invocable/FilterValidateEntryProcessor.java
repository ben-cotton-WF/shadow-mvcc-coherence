package com.sixwhits.cohmvcc.invocable;

import static com.sixwhits.cohmvcc.domain.IsolationLevel.readUncommitted;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.Constants;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.ProcessorResult;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.Entry;

@Portable
public class FilterValidateEntryProcessor<K> extends AbstractMVCCProcessor<K, VersionedKey<K>> {

	private static final long serialVersionUID = -954213053828163546L;

	public FilterValidateEntryProcessor() {
		super();
	}

	public FilterValidateEntryProcessor(TransactionId transactionId,
			IsolationLevel isolationLevel, CacheName cacheName,
			Filter validationFilter) {
		super(transactionId, isolationLevel, cacheName, validationFilter);
	}


	@SuppressWarnings("unchecked")
	@Override
	public ProcessorResult<K, VersionedKey<K>> process(Entry entryarg) {
		
		BinaryEntry entry = (BinaryEntry) entryarg;
		
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
		
		ReadOnlyEntryWrapper childEntry = new ReadOnlyEntryWrapper(entry, transactionId, isolationLevel, cacheName);
		
		if (!confirmFilterMatch(childEntry)) {
			return null;
		}
		
		return new ProcessorResult<K, VersionedKey<K>>((VersionedKey<K>)priorEntry.getKey(), null);
	}

}
